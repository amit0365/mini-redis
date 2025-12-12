use std::{collections::{BTreeSet, HashMap, HashSet, VecDeque}, marker::PhantomData, sync::{Arc, Mutex, RwLock}, time::{Duration, Instant}};
use ordered_float::OrderedFloat;
use tokio::{sync::mpsc::{self, Receiver, Sender, error::TrySendError}, time::sleep};
use serde_json::{json, Value};

use crate::{error::{RedisError, RedisResult}, protocol::{RedisValue, StreamValue, value::redis_value_as_string}, utils::{collect_as_strings, decode_score_to_coordinates, encode_coordinates_to_score, encode_resp_array_arc, encode_resp_array_str, encode_resp_ref_array_arc, encode_resp_value_array, parse_wrapback}};

#[derive(Clone)]
pub struct RedisState<K, RedisValue> {
    channels_state: ChannelState<K>,
    map_state: MapState<K, RedisValue>,
    list_state: ListState<K, RedisValue>,
    sorted_set_state: SortedSetState<K>,
    server_state: ServerState<K, RedisValue>
}

impl<K, RedisValue> RedisState<K, RedisValue> {
    pub fn channels_state(&self) -> &ChannelState<K> {
        &self.channels_state
    }

    pub fn map_state(&self) -> &MapState<K, RedisValue> {
        &self.map_state
    }

    pub fn list_state(&self) -> &ListState<K, RedisValue> {
        &self.list_state
    }

    pub fn server_state(&self) -> &ServerState<K, RedisValue> {
        &self.server_state
    }

    pub fn server_state_mut(&mut self) -> &mut ServerState<K, RedisValue> {
        &mut self.server_state
    }
}

#[derive(Clone)]
pub struct ChannelState<K>{
    channels_map: Arc<RwLock<HashMap<K, (usize, HashSet<Arc<str>>)>>>,
    subscribers: Arc<Mutex<HashMap<K, Vec<Sender<(Arc<str>, Arc<Vec<Arc<str>>>)>>>>>,
}

impl<K> ChannelState<K>{
    fn new() -> Self{
        let channels_map = Arc::new(RwLock::new(HashMap::new()));
        let subscribers = Arc::new(Mutex::new(HashMap::new()));
        ChannelState { channels_map, subscribers }
    }
}

#[derive(Clone)]
pub struct ServerState<K, V>{
    replication_mode: Arc<Mutex<bool>>,
    map: HashMap<K, V>
}

impl<K, V> ServerState<K, V> {
    pub fn replication_mode(&self) -> bool {
        *self.replication_mode.lock().expect("replication_mode mutex poisoned")
    }

    pub fn set_replication_mode(&mut self, mode: bool) {
        *self.replication_mode.lock().expect("replication_mode mutex poisoned") = mode;
    }

    pub fn map(&self) -> &HashMap<K, V> {
        &self.map
    }

    pub fn map_mut(&mut self) -> &mut HashMap<K, V> {
        &mut self.map
    }
}

#[derive(Clone)]
pub struct ReplicasState{
    num_connected_replicas: Arc<RwLock<usize>>,
    replica_offsets: Arc<RwLock<HashMap<usize, usize>>>,
    master_write_offset: Arc<RwLock<usize>>,
    replica_senders: Arc<Mutex<HashMap<usize, Sender<Arc<str>>>>>,
    ack_tx: mpsc::Sender<(usize, usize)>,
    ack_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<(usize, usize)>>>,
}

impl ReplicasState {
    pub fn new() -> Self {
        let (ack_tx, ack_rx) = mpsc::channel(1);
        ReplicasState {
            num_connected_replicas: Arc::new(RwLock::new(0)),
            replica_offsets: Arc::new(RwLock::new(HashMap::new())),
            master_write_offset: Arc::new(RwLock::new(0)),
            replica_senders: Arc::new(Mutex::new(HashMap::new())),
            ack_tx,
            ack_rx: Arc::new(tokio::sync::Mutex::new(ack_rx))
        }
    }

    pub fn increment_master_write_offset(&mut self, n: usize){
       *self.master_write_offset.write().expect("master_write_offset lock poisoned") += n;
    }

    pub fn init_replica_offset(&self, id: usize) {
        self.replica_offsets.write().expect("replica_offsets lock poisoned").insert(id, 0);
    }

    pub fn update_replica_offsets(&mut self, id: usize, offset: usize){
        self.replica_offsets.write().expect("replica_offsets lock poisoned")
            .entry(id).and_modify(|v| *v = offset);
    }

    pub fn get_replica_offset(&self, id: usize) -> Option<usize> {
        self.replica_offsets.read().expect("replica_offsets lock poisoned").get(&id).copied()
    }

    pub fn get_master_write_offset(&self) -> usize {
        *self.master_write_offset.read().expect("master_write_offset lock poisoned")
    }

    pub fn num_connected_replicas(&self) -> usize {
        *self.num_connected_replicas.read().expect("num_connected_replicas lock poisoned")
    }

    pub fn increment_num_connected_replicas(&mut self){
       *self.num_connected_replicas.write().expect("num_connected_replicas lock poisoned") += 1;
    }

    pub fn replica_senders(&self) -> &Arc<Mutex<HashMap<usize, Sender<Arc<str>>>>> {
        &self.replica_senders
    }

    pub fn ack_tx(&self) -> &mpsc::Sender<(usize, usize)> {
        &self.ack_tx
    }

    pub fn ack_rx(&self) -> &Arc<tokio::sync::Mutex<mpsc::Receiver<(usize, usize)>>> {
        &self.ack_rx
    }
}

impl ServerState<Arc<str>, RedisValue>{
    fn new() -> Self{
        ServerState { replication_mode: Arc::new(Mutex::new(false)), map: HashMap::new() }
    }

    pub fn update(&mut self, pairs: Vec<(Arc<str>, RedisValue)>){
        for (k, v) in pairs {
            self.map_mut().insert(k, v);
        }
    }
}

//subscribe and preokicaiton state should be optional
pub struct ClientState<K, V>{
    queued_state: QueuedState<K, V>,
    subscription_state: SubscriptionState<K, V>,
    replication_state: ReplicationState<K, V>,
}

pub struct SubscriptionState<K, V>{
    subscribe_mode: bool,
    map: (usize, HashSet<V>),
    receiver: Option<Receiver<(K, Arc<Vec<V>>)>>,
    sender: Option<Sender<(K, Arc<Vec<V>>)>>,
}

impl<K, V> SubscriptionState<K, V> {
    fn new() -> Self {
        SubscriptionState {
            subscribe_mode: false,
            map: (0, HashSet::new()),
            receiver: None,
            sender: None,
        }
    }

    pub fn is_subscribe_mode(&self) -> bool {
        self.subscribe_mode
    }

    pub fn set_subscribe_mode(&mut self, mode: bool) {
        self.subscribe_mode = mode;
    }

    pub fn get_map(&self) -> &(usize, HashSet<V>) {
        &self.map
    }

    pub fn get_map_mut(&mut self) -> &mut (usize, HashSet<V>) {
        &mut self.map
    }

    pub fn get_receiver_mut(&mut self) -> Option<&mut Receiver<(K, Arc<Vec<V>>)>> {
        self.receiver.as_mut()
    }

    pub fn get_sender(&self) -> &Option<Sender<(K, Arc<Vec<V>>)>> {
        &self.sender
    }

    pub fn set_channel(&mut self, sender: Sender<(K, Arc<Vec<V>>)>, receiver: Receiver<(K, Arc<Vec<V>>)>) {
        self.sender = Some(sender);
        self.receiver = Some(receiver);
    }

    pub fn has_receiver(&self) -> bool {
        self.receiver.is_none()
    }
}

pub struct ReplicationState<K, V>{
    is_replica: bool,
    replica_id: usize,
    num_bytes_synced: usize,
    receiver: Option<Receiver<V>>,
    _phantom: PhantomData<(K, V)>
}

impl<K, V> ReplicationState<K, V> {
    fn new() -> Self {
        ReplicationState {
            replica_id: 0,
            is_replica: false,
            num_bytes_synced: 0,
            receiver: None,
            _phantom: PhantomData,
        }
    }

    pub fn set_replica_id(&mut self, id: usize){
        self.replica_id = id;
    }

    pub fn get_replica_id(&self) -> usize{
        self.replica_id
    }

    pub fn is_replica(&self) -> bool {
        self.is_replica
    }

    pub fn set_replica(&mut self, is_replica: bool) {
        self.is_replica = is_replica;
    }

    pub fn num_bytes_synced(&self) -> usize{
        self.num_bytes_synced
    }

    pub fn add_num_bytes_synced(&mut self, n :usize){
        self.num_bytes_synced += n;
    }

    pub fn get_receiver_mut(&mut self) -> Option<&mut Receiver<V>> {
        self.receiver.as_mut()
    }

    pub fn set_receiver(&mut self, receiver: Receiver<V>) {
        self.receiver = Some(receiver);
    }
}

pub struct QueuedState<K, V>{
    multi_queue_mode: bool,
    queued_commands: VecDeque<Vec<V>>,
    _phantom: PhantomData<K>,
}

impl<K, V> QueuedState<K, V> {
    fn new() -> Self {
        QueuedState {
            multi_queue_mode: false,
            queued_commands: VecDeque::new(),
            _phantom: PhantomData,
        }
    }

    pub fn is_multi_queue_mode(&self) -> bool {
        self.multi_queue_mode
    }

    pub fn set_multi_queue_mode(&mut self, mode: bool) {
        self.multi_queue_mode = mode;
    }

    pub fn push_command(&mut self, command: Vec<V>) {
        self.queued_commands.push_back(command);
    }

    pub fn pop_command(&mut self) -> Option<Vec<V>> {
        self.queued_commands.pop_front()
    }

    pub fn clear_commands(&mut self) {
        self.queued_commands.clear();
    }

    pub fn commands_len(&self) -> usize {
        self.queued_commands.len()
    }
}

impl ClientState<Arc<str>, Arc<str>>{
    pub fn new() -> Self{
        ClientState {
            queued_state: QueuedState::new(),
            subscription_state: SubscriptionState::new(),
            replication_state: ReplicationState::new(),
        }
    }

    pub fn set_replica_id(&mut self, id: usize){
        self.replication_state.set_replica_id(id);
    }

    pub fn get_replica_id(&self) -> usize {
        self.replication_state.get_replica_id()
    }

    // Delegation methods for SubscriptionState
    pub fn is_subscribe_mode(&self) -> bool {
        self.subscription_state.is_subscribe_mode()
    }

    pub fn set_subscribe_mode(&mut self, mode: bool) {
        self.subscription_state.set_subscribe_mode(mode);
    }

    pub fn get_subscriptions(&self) -> &(usize, HashSet<Arc<str>>) {
        self.subscription_state.get_map()
    }

    pub fn get_subscriptions_mut(&mut self) -> &mut (usize, HashSet<Arc<str>>) {
        self.subscription_state.get_map_mut()
    }

    pub fn get_sub_receiver_mut(&mut self) -> Option<&mut Receiver<(Arc<str>, Arc<Vec<Arc<str>>>)>> {
        self.subscription_state.get_receiver_mut()
    }

    pub fn get_sub_sender(&self) -> &Option<Sender<(Arc<str>, Arc<Vec<Arc<str>>>)>> {
        self.subscription_state.get_sender()
    }

    pub fn get_replica_receiver_mut(&mut self) -> Option<&mut Receiver<Arc<str>>> {
        self.replication_state.get_receiver_mut()
    }

    pub fn set_channel(&mut self, sender: Sender<(Arc<str>, Arc<Vec<Arc<str>>>)>, receiver: Receiver<(Arc<str>, Arc<Vec<Arc<str>>>)>) {
        self.subscription_state.set_channel(sender, receiver);
    }

    pub fn has_receiver(&self) -> bool {
        self.subscription_state.has_receiver()
    }

    // Delegation methods for QueuedState
    pub fn is_multi_queue_mode(&self) -> bool {
        self.queued_state.is_multi_queue_mode()
    }

    pub fn set_multi_queue_mode(&mut self, mode: bool) {
        self.queued_state.set_multi_queue_mode(mode);
    }

    pub fn push_command(&mut self, command: Vec<Arc<str>>) {
        self.queued_state.push_command(command);
    }

    pub fn pop_command(&mut self) -> Option<Vec<Arc<str>>> {
        self.queued_state.pop_command()
    }

    pub fn clear_commands(&mut self) {
        self.queued_state.clear_commands();
    }

    pub fn commands_len(&self) -> usize {
        self.queued_state.commands_len()
    }

    // Delegation methods for ReplicationState
    pub fn is_replica(&self) -> bool {
        self.replication_state.is_replica()
    }

    pub fn set_replica(&mut self, is_replica: bool) {
        self.replication_state.set_replica(is_replica);
    }

    pub fn num_bytes_synced(&self) -> usize {
        self.replication_state.num_bytes_synced()
    }

    pub fn add_num_bytes_synced(&mut self, n :usize){
        self.replication_state.add_num_bytes_synced(n);
    }

    pub fn set_replica_receiver(&mut self, receiver: Receiver<Arc<str>>){
        self.replication_state.set_receiver(receiver);
    }
}

#[derive(Clone)]
pub struct ListState<K, RedisValue>{
    list: Arc<Mutex<HashMap<K, VecDeque<RedisValue>>>>,
    waiters: Arc<Mutex<HashMap<K, VecDeque<Sender<(Arc<str>, RedisValue)>>>>>,
}

impl<K> ListState<K, RedisValue>{
    fn new() -> Self{
        let list = Arc::new(Mutex::new(HashMap::new()));
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        ListState { list, waiters }
    }
}

#[derive(Clone)]
pub struct MapState<K, RedisValue>{
    map: Arc<RwLock<HashMap<K, RedisValue>>>,
    waiters: Arc<Mutex<HashMap<K, VecDeque<Sender<(Arc<str>, RedisValue)>>>>>,
}

impl<K> MapState<K, RedisValue>{
    fn new() -> Self{
        let map = Arc::new(RwLock::new(HashMap::new()));
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        MapState { map, waiters }
    }
}

#[derive(Clone)]
pub struct SortedSetState<K>{
    set: Arc<RwLock<HashMap<K, SortedSet>>>,
}

#[derive(Clone)]
pub struct SortedSet{
    members: HashMap<Arc<str>, f64>,
    scores: BTreeSet<(OrderedFloat<f64>, Arc<str>)>,
}

impl SortedSet{
    fn new() -> Self{
        let members = HashMap::new();
        let scores = BTreeSet::new();
        SortedSet { members, scores }
    }
}

impl<K> SortedSetState<K>{
    fn new() -> Self{
        let set = Arc::new(RwLock::new(HashMap::new()));
        SortedSetState { set }
    }
}

impl RedisState<Arc<str>, RedisValue>{
    pub fn new() -> Self{
        let channels_state = ChannelState::new();
        let map_state = MapState::new();
        let list_state = ListState::new();
        let server_state = ServerState::new();
        let sorted_set_state = SortedSetState::new();
        RedisState { channels_state, map_state, list_state, server_state, sorted_set_state }
    }

    pub fn psync(&self) -> RedisResult<String> {
        let repl_id = self.server_state().map().get("master_replid")
            .ok_or_else(|| RedisError::KeyNotFound("master_replid not found".to_string()))?;
        let offset = self.server_state().map().get("master_repl_offset")
            .ok_or_else(|| RedisError::KeyNotFound("master_repl_offset not found".to_string()))?;
        let fullresync_response = format!("+FULLRESYNC {} {}\r\n", repl_id, offset);
        Ok(fullresync_response)
    }

    pub fn set(&mut self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let key = Arc::clone(&commands[1]);
        let value = Arc::clone(&commands[2]);
        match commands.iter().skip(3).next() {
            Some(str) => {
                match str.to_uppercase().as_str() {
                    "PX" => {
                        let timeout_ms: u64 = commands[4].parse()?;
                        let timeout = Instant::now() + Duration::from_millis(timeout_ms);
                        self.map_state().map.write()?.insert(key, RedisValue::StringWithTimeout((value, timeout)));
                    }
                    "EX" => {
                        let timeout_s: u64 = commands[4].parse()?;
                        let timeout = Instant::now() + Duration::from_secs(timeout_s);
                        self.map_state().map.write()?.insert(key, RedisValue::StringWithTimeout((value, timeout)));
                    }
                    _ => (),
                }
            }
            None => {
                let redis_val = match value.parse::<u64>(){
                    Ok(num) => RedisValue::Number(num),
                    Err(_) => RedisValue::String(value),
                };

                self.map_state().map.write()?.insert(key, redis_val);
            }
        }

        Ok("+OK\r\n".to_string())
    }

    pub fn get(&mut self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let value = self.map_state().map.read()?.get(&commands[1]).cloned();
        if let Some(value) = value {
            match value {
                RedisValue::StringWithTimeout((value, timeout)) => {
                    if Instant::now() < timeout {
                        Ok(format!("${}\r\n{}\r\n", value.len(), value))
                    } else {
                        Ok("$-1\r\n".to_string())
                    }
                }
                RedisValue::String(val) => {
                    Ok(format!("${}\r\n{}\r\n", val.len(), val))
                }
                RedisValue::Number(val) => {
                    Ok(format!("${}\r\n{}\r\n", val.to_string().len(), val))
                }
                _ => Ok("$-1\r\n".to_string()) // fix error handling
            }
        } else { Ok("$-1\r\n".to_string())}
    } 

    pub fn rpush(&mut self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let key = &commands[1];
        let count = {
            let mut list_guard = self.list_state().list.lock()?;
            let items = commands
                .iter()
                .skip(2)
                .map(|v| RedisValue::String(Arc::clone(v)));
            list_guard
                .entry(Arc::clone(key))
                .or_insert(VecDeque::new())
                .extend(items);
            list_guard.get(key)
                .ok_or_else(|| RedisError::KeyNotFound(format!("Key {} not found", key)))?
                .len()
                .to_string()
        };

        let mut waiters_guard = self.list_state().waiters.lock()?;
        if let Some(waiting_queue) = waiters_guard.get_mut(key) {
            let key_arc = Arc::clone(key);
            while let Some(sender) = waiting_queue.pop_front() {
                let mut list_guard = self.list_state().list.lock()?;
                if let Some(deque) = list_guard.get_mut(key) {
                    if let Some(value) = deque.pop_front() {
                        drop(list_guard); // Release lock before sending
                        match sender.try_send((Arc::clone(&key_arc), value)) {
                            Ok(_) => break,
                            Err(TrySendError::Full(_)) => return Err(RedisError::TooManyWaiters),
                            Err(TrySendError::Closed(_)) => continue,
                        }
                    }
                }
            }
        }

        Ok(format!(":{}\r\n", count))
    } 

    pub fn lpush(&mut self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let mut list_guard = self.list_state().list.lock()?;
        let items = commands
            .iter()
            .skip(2)
            .map(|v| RedisValue::String(Arc::clone(v)));
        let key = &commands[1];
        let deque = list_guard.entry(Arc::clone(key)).or_insert(VecDeque::new());

        for item in items.into_iter() {
            deque.push_front(item);
        }

        let count = list_guard.get(key)
            .ok_or_else(|| RedisError::KeyNotFound(format!("Key {} not found", key)))?
            .len();
        Ok(format!(":{}\r\n", count.to_string()))
    } 

    pub fn llen(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let list_guard = self.list_state().list.lock()?;
        let len = match list_guard.get(&commands[1]) {
            Some(list) => list.len().to_string(),
            None => 0.to_string(),
        };

        Ok(format!(":{}\r\n", len))
    } 

    pub fn lpop(&mut self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let mut list_guard = self.list_state().list.lock()?;
        match list_guard.get_mut(&commands[1]){
            Some(list) => {
                match commands.iter().skip(2).next(){
                    Some(n) => {
                        let mut popped_list = Vec::new();
                        let len = n.parse::<usize>()?;
                        for _ in 0..len{
                            match list.pop_front(){
                                Some(popped) => {
                                    if let Some(val) = redis_value_as_string(popped){
                                        popped_list.push(val);
                                    }
                                }
                                None => (),
                            }
                        }

                        Ok(encode_resp_array_arc(&popped_list))
                    },

                    None => {
                        match list.pop_front(){
                            Some(popped) => {
                                if let Some(val) = popped.as_string(){
                                    Ok(format!("${}\r\n{}\r\n", val.len(), val))
                                } else {
                                    Ok(format!("$-1\r\n"))
                                }
                            }
                            None => Ok(format!("$-1\r\n")),
                        }
                    },
                }
            },

            None => Ok(format!("$-1\r\n"))
        }
    } 

    pub async fn blpop(&mut self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let key = &commands[1];

        {
            let mut list_guard = self.list_state().list.lock()?;
            if let Some(list) = list_guard.get_mut(key){
                if let Some(data) = list.pop_front(){
                    if let Some(val) = data.as_string(){
                        return Ok(encode_resp_array_arc(&[Arc::clone(key), Arc::clone(val)]))
                    }
                }
            }
        }

        let mut receiver = {
            let mut waiters_guard = self.list_state().waiters.lock()?;
            let queue = waiters_guard.entry(Arc::clone(key)).or_insert(VecDeque::new());
            if queue.len() > 10000 {
                return Err(RedisError::Other("ERR_TOO_MANY_BLPOP_WAITERS_FOR_THE_KEY".to_string()))
            }

            let (sender, receiver) = mpsc::channel(1);
            queue.push_back(sender);
            receiver
        };

        // Re-check the list after adding to waiters to avoid race condition
        // where RPUSH adds item between our first check and adding to waiters
        // {
        //     let mut list_guard = self.list_state().list.lock().unwrap();
        //     if let Some(list) = list_guard.get_mut(key){
        //         if let Some(data) = list.pop_front(){
        //             // Remove ourselves from waiters since we got the item directly
        //             let mut waiters_guard = self.list_state().waiters.lock().unwrap();
        //             if let Some(queue) = waiters_guard.get_mut(key){
        //                 // Remove the last sender we just added
        //                 queue.pop_back();
        //             }

        //             if let Some(val) = data.as_string(){
        //                 return encode_resp_array(&vec![key.clone(), val.clone()])
        //             }
        //         }
        //     }
        // }

        let timeout: f64 = commands.last()
            .ok_or_else(|| RedisError::InvalidCommand("BLPOP requires timeout argument".to_string()))?
            .parse()?;
        if timeout == 0.0 {
            match receiver.recv().await{
                Some((key_arc, value)) => {
                    if let Some(val) = value.as_string(){
                        Ok(encode_resp_array_str(&[key_arc.as_ref(), val]))
                    } else {
                        Ok(format!("*-1\r\n"))
                    }
                }
                None => Ok(format!("*-1\r\n")),
            }
        } else {
            tokio::select! {
                result = receiver.recv() => {
                    if let Some((key_arc, value)) = result{
                        if let Some(val) = value.as_string(){
                            Ok(encode_resp_array_str(&[key_arc.as_ref(), val]))
                        } else {
                            Ok(format!("*-1\r\n"))
                        }
                    } else {
                        Ok(format!("*-1\r\n"))
                    }
                }

                _ = sleep(Duration::from_secs_f64(timeout)) => {
                    Ok(format!("*-1\r\n"))
                }
            }
        }
    }
    
    pub fn lrange(&self, key: &Arc<str>, start: &Arc<str>, stop: &Arc<str>) -> RedisResult<String> {
        let list_guard = self.list_state().list.lock()?;
        let array = match list_guard.get(key){
            Some(vec) => {
                let start = parse_wrapback(start.parse::<i64>()?, vec.len())?;
                let stop = parse_wrapback(stop.parse::<i64>()?, vec.len())?;

                if start >= vec.len(){
                    VecDeque::new()
                } else if stop >= vec.len(){
                    vec.range(start..).cloned().collect()
                } else if start > stop{
                    VecDeque::new() // removed stop=start should be covered by last else
                } else {
                    vec.range(start..=stop).cloned().collect()
                }
            },
            None => VecDeque::new()
        };

        Ok(encode_resp_array_arc(&collect_as_strings(array)))
    } 

    pub fn type_command(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let map_guard = self.map_state().map.read()?;
        let response = match map_guard.get(&commands[1]){
            Some(val) => {
                match val{
                    RedisValue::String(_) => "string".to_string(),
                    RedisValue::StringWithTimeout(_) => "string".to_string(),
                    RedisValue::Stream(_) => "stream".to_string(),
                    RedisValue::Number(_) => "number".to_string(),
                }
            },
            None => "none".to_string()
        };

        Ok(format!("+{}\r\n", response))
    } 

    pub fn xadd(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let key = &commands[1];

        let has_waiters = {
            let map_waiters_guard = self.map_state().waiters.lock()?;
            map_waiters_guard.get(key).map_or(false, |q| !q.is_empty())
        };

        let pairs_grouped = Arc::new(
            commands[3..].chunks_exact(2)
                .map(|chunk| (Arc::clone(&chunk[0]), Arc::clone(&chunk[1])))
                .collect::<Vec<_>>()
        );

        let result = {
            let mut map_guard = self.map_state().map.write()?;
            map_guard
                .entry(Arc::clone(key))
                .or_insert(RedisValue::Stream(StreamValue::new()))
                .update_stream(&commands[2], Arc::clone(&pairs_grouped))?
        };

        if has_waiters {
            let mut map_waiters_guard = self.map_state().waiters.lock()?;
            if let Some(waiters_queue) = map_waiters_guard.get_mut(key) {
                let stream_value = StreamValue::new_blocked(Arc::clone(&commands[2]), pairs_grouped);
                while let Some(waiter) = waiters_queue.pop_front() {
                    match waiter.try_send((Arc::clone(key), RedisValue::Stream(stream_value.clone()))) {
                        Ok(_) => break,
                        Err(TrySendError::Full(_)) => return Err(RedisError::TooManyWaiters),
                        Err(TrySendError::Closed(_)) => continue,
                    }
                }
            }
        }

        Ok(result)
    } 

    pub fn xrange(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let mut encoded_array = String::new();
        let map_guard = self.map_state().map.read()?;
        let stream_value = map_guard.get(&commands[1])
            .ok_or_else(|| RedisError::KeyNotFound(format!("Key {} not found", commands[1])))?;
        let values = stream_value.get_stream_range(&commands[2], Some(&commands[3]))?;
        if !values.is_empty() {
            encode_resp_value_array(&mut encoded_array, &values);
            Ok(encoded_array)
        } else {
            Ok(format!("*-1\r\n"))
        }
    } 

    pub async fn xread(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        match commands[1].as_ref(){
            "streams" => {
                let map_guard = self.map_state().map.read()?;
                let key_tokens = commands.iter().skip(2)
                .filter(|token| map_guard.get(*token).is_some())
                .collect::<Vec<_>>();

                let id_tokens = commands.iter().skip(2 + key_tokens.len()).collect::<Vec<_>>();
                let mut encoded_array = String::new();
                let mut key_entries = Vec::new();

                for (key, id) in key_tokens.iter().zip(id_tokens){
                    let values = map_guard
                    .get(*key)
                    .ok_or_else(|| RedisError::KeyNotFound(format!("Key {} not found", key.as_ref())))?
                    .get_stream_range(id, None)?;
                    if !values.is_empty() { key_entries.push(json!([key.as_ref(), values]));}
                }
        
                if !key_entries.is_empty() {
                    encode_resp_value_array(&mut encoded_array, &key_entries);
                    Ok(encoded_array)
                } else {
                    Ok(format!("*-1\r\n"))
                }
            },

            "block" => {
                let timeout = commands[2].parse::<u64>()?;
                let key = &commands[4];

                let mut receiver = {
                    let mut waiters_guard = self.map_state().waiters.lock()?;
                    let queue = waiters_guard.entry(Arc::clone(key)).or_insert(VecDeque::new());
                    if queue.len() > 10000{
                        return Err(RedisError::Other("ERR_TOO_MANY_XREAD_WAITERS_FOR_THE_KEY".to_string()))
                    }

                    let (sender, receiver) = mpsc::channel(1);
                    queue.push_back(sender);
                    receiver
                };

                if timeout == 0 {
                    match receiver.recv().await{
                        Some((key_arc, redis_val)) => {
                            if let Some(value_array) = redis_val.get_blocked_result(){
                                let mut encoded_array = String::new();
                                encode_resp_value_array(&mut encoded_array, &vec![json!([key_arc.as_ref(), value_array])]);
                                return Ok(encoded_array)
                            }
                            Ok(format!("*-1\r\n"))
                        },
                        None => Ok(format!("*-1\r\n"))
                    }
                } else {
                    tokio::select! {
                        result = receiver.recv() => {
                            if let Some((key_arc, redis_val)) = result {
                                if let Some(value_array) = redis_val.get_blocked_result(){
                                    let mut encoded_array = String::new();
                                    encode_resp_value_array(&mut encoded_array, &vec![json!([key_arc.as_ref(), value_array])]);
                                    return Ok(encoded_array)
                                }
                                Ok(format!("*-1\r\n"))
                            } else {
                                Ok(format!("*-1\r\n"))
                            }
                        },

                        _ = sleep(Duration::from_millis(timeout)) => {
                            Ok(format!("*-1\r\n"))
                        },
                    }
                }
            },

            _ => Err(RedisError::InvalidCommand("Invalid XREAD subcommand".to_string()))
        }
    }

    pub fn incr(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let mut map_guard = self.map_state().map.write()?;
        let val = map_guard.entry(Arc::clone(&commands[1])).or_insert(RedisValue::Number(0));
        match val{
            RedisValue::Number(n) => {
                *n += 1;
                Ok(format!(":{}\r\n", n))
            },
            _ => Ok(format!("-ERR value is not an integer or out of range\r\n")), //tester expects this format
        }
    } 

    pub fn multi(&self, client_state: &mut ClientState<Arc<str>, Arc<str>>) -> RedisResult<String> {
        client_state.set_multi_queue_mode(true);
        Ok(format!("+OK\r\n"))
    } 

    pub fn info(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        match commands[1].to_uppercase().as_str(){
            "REPLICATION" => {
                let mut lines = Vec::new();
                for (key, value) in self.server_state().map().iter() {
                    lines.push(format!("{}:{}", key, value));
                }
                let content = lines.join("\r\n");
                Ok(format!("${}\r\n{}\r\n", content.len(), content))
            }
            _ => Err(RedisError::InvalidCommand(format!("INFO subcommand '{}' not supported", commands[1]))),
        }
    } 

    pub fn zadd(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let key = Arc::clone(&commands[1]);
        let mut sorted_state_guard = self.sorted_set_state.set.write()?;
        let sorted_state = sorted_state_guard.entry(key).or_insert_with(|| SortedSet::new());

        let mut new_members = 0;
        let args = &commands[2..];
        for i in (0..args.len()).step_by(2){
            let score = args[i].parse::<f64>()?;
            let member = &args[i+1];
            if let Some(old_score) = sorted_state.members.get_mut(member){
                sorted_state.scores.remove(&(OrderedFloat::from(*old_score), Arc::clone(member)));
                *old_score = score;
            } else {
                sorted_state.members.insert(Arc::clone(&member), score);
                new_members += 1;
            }

            sorted_state.scores.insert((OrderedFloat::from(score), Arc::clone(&member)));
        }

        Ok(format!(":{}\r\n", new_members))
    } 

    pub fn zrank(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let key = &commands[1];
        let memeber_name = &commands[2];
        let sorted_state_guard = self.sorted_set_state.set.read()?;
        match sorted_state_guard.get(key){
            Some(sorted_state) => {
                match sorted_state.scores.iter().position(|(_, member)| member == memeber_name){
                    Some(rank) => Ok(format!(":{}\r\n", rank)),
                    None => Ok(format!("$-1\r\n"))
                }
            }
            None => Ok(format!("$-1\r\n"))
        }
    }

    pub fn zrange(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let key = &commands[1];
        let empty_array = format!("*0\r\n");
        let sorted_state_guard = self.sorted_set_state.set.read()?;
        match sorted_state_guard.get(key){
            Some(sorted_state) => {
                let set_len = sorted_state.scores.len();
                let start = parse_wrapback(commands[2].parse::<i64>()?, set_len)?;
                let stop = parse_wrapback(commands[3].parse::<i64>()?, set_len)?;

                if start >= sorted_state.scores.len(){
                    Ok(empty_array)
                } else if start > stop {
                    Ok(empty_array)
                } else if stop >= sorted_state.scores.len(){
                    let elements = sorted_state.scores.iter().skip(start).take(set_len - start).map(|(_score, name)| name).collect::<Vec<_>>();
                    Ok(encode_resp_ref_array_arc(&elements))
                } else {
                    let elements = sorted_state.scores.iter().skip(start).take(stop - start + 1).map(|(_score, name)| name).collect::<Vec<_>>();
                    Ok(encode_resp_ref_array_arc(&elements))
                }
            }
            None => Ok(empty_array)
        }
    }

    pub fn zcard(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let key = &commands[1];
        let sorted_state_guard = self.sorted_set_state.set.read()?;
        match sorted_state_guard.get(key){
            Some(sorted_state) => Ok(format!(":{}\r\n", sorted_state.scores.len())),
            None => Ok(format!(":0\r\n"))
        }
    }

    pub fn zscore(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let (key, member) = (&commands[1], &commands[2]);
        let sorted_state_guard = self.sorted_set_state.set.read()?;
        match sorted_state_guard.get(key){
            Some(sorted_state) => {
                match sorted_state.members.get(member){
                    Some(score) => {
                        let score_str = score.to_string();
                        Ok(format!("${}\r\n{}\r\n", score_str.len(), score_str))
                    },
                    None => Ok(format!(":-1\r\n")),
                }
            },
            None => Ok(format!(":-1\r\n"))
        }
    }

    pub fn zrem(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let (key, member) = (&commands[1], &commands[2]);
        let mut sorted_state_guard = self.sorted_set_state.set.write()?;
        match sorted_state_guard.get_mut(key){
            Some(sorted_state) => {
                if let Some(score) = sorted_state.members.remove(member){
                    sorted_state.scores.remove(&(OrderedFloat::from(score), Arc::clone(member)));
                    Ok(format!(":1\r\n"))
                } else { Ok(format!(":0\r\n")) }
            },
            None => Ok(format!(":0\r\n"))
        }
    }

    pub fn geoadd(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let (key, longitude_str, latitude_str, member) = (&commands[1], &commands[2], &commands[3], &commands[4]);
        let (longitude, latitude) = (longitude_str.parse::<f64>()?, latitude_str.parse::<f64>()?);
        let mut sorted_state_guard = self.sorted_set_state.set.write()?;

        if -180.0 >= longitude || longitude >= 180.0 {
            Ok(format!("-ERR longitude is invlaid\r\n"))
        } else if -85.05112878 >= latitude || latitude >= 85.05112878 {
            Ok(format!("-ERR latitude is invlaid\r\n"))
        } else { 
            let sorted_state = sorted_state_guard.entry(Arc::clone(key)).or_insert_with(|| SortedSet::new());
            let score = encode_coordinates_to_score(latitude, longitude) as f64;
            sorted_state.members.insert(Arc::clone(&member), score);
            sorted_state.scores.insert((OrderedFloat::from(0), Arc::clone(&member)));

            Ok(format!(":1\r\n"))
        }
    }

    pub fn geopos(&self, commands: &Vec<Arc<str>>) -> RedisResult<String> {
        let (key, members) = (&commands[1], &commands[2..]);
        let sorted_state_guard = self.sorted_set_state.set.read()?;
        match sorted_state_guard.get(key){
            Some(sorted_state) => {
                let mut coordinates_array= Vec::new();
                for member in members {
                    match sorted_state.members.get(member){
                        Some(score) => {
                            let coordinates = decode_score_to_coordinates(*score as u64);
                            coordinates_array.push(coordinates.as_value());
                        },
                        None => coordinates_array.push(Value::Null),
                    }
                }

                let mut resp_array = String::new(); 
                encode_resp_value_array(&mut resp_array, &coordinates_array);
                Ok(resp_array)
            },
            None => Ok(format!("*1\r\n*-1\r\n"))
        }
    }

    pub fn subscribe(&mut self, client_state: &mut ClientState<Arc<str>, Arc<str>>, client: &Arc<str>, commands: &Vec<Arc<str>>) -> RedisResult<String>{
        client_state.set_subscribe_mode(true);
        let subs_count = if client_state.get_subscriptions().1.contains(&commands[1]){
            client_state.get_subscriptions().0
        } else {
            let mut channel_guard = self.channels_state().channels_map.write()?;
            let (count, client_set) = channel_guard.entry(Arc::clone(&commands[1])).or_insert((0, HashSet::new()));
            *count += 1;
            client_set.insert(Arc::clone(client));

            let subscriptions = client_state.get_subscriptions_mut();
            subscriptions.1.insert(Arc::clone(&commands[1]));
            subscriptions.0 += 1;
            subscriptions.0
        };

        Ok(format!("*3\r\n${}\r\n{}\r\n${}\r\n{}\r\n:{}\r\n", commands[0].len(), commands[0].to_lowercase(), commands[1].len(), commands[1], subs_count))
    }

    pub async fn handle_subscriber(&self, client_state: &mut ClientState<Arc<str>, Arc<str>>, commands: &Vec<Arc<str>>) -> RedisResult<()>{
        if client_state.has_receiver() {
            let (sender, receiver) = mpsc::channel(1000);
            client_state.set_channel(sender, receiver);
        }

        if let Some(sender) = client_state.get_sub_sender() {
            let mut subs_guard = self.channels_state().subscribers.lock()?;
            subs_guard.entry(Arc::clone(&commands[1])).or_insert(Vec::new()).push(sender.clone());
        }
        Ok(())
    }

    pub fn publish(&self, commands: &Vec<Arc<str>>) -> RedisResult<String>{
        let channel_guard = self.channels_state().channels_map.read()?;
        let subs = channel_guard.get(&commands[1])
            .map(|(count, _)| *count)
            .unwrap_or(0); // todo cehck this
        let channel_name = &commands[1];
        let messages = Arc::new(commands.iter().skip(2).cloned().collect::<Vec<_>>());
        drop(channel_guard);
        
        let mut subs_guard = self.channels_state().subscribers.lock()?;
        if let Some(subs) = subs_guard.get_mut(&commands[1]){
            subs.retain(|sender|
                match sender.try_send((Arc::clone(channel_name), Arc::clone(&messages))){
                    Ok(_) => true,
                    Err(TrySendError::Full(_)) => true,
                    Err(TrySendError::Closed(_)) => false,
            })
        }

        Ok(format!(":{}\r\n", subs))
    }

    pub fn unsubscribe(&self, client_state: &mut ClientState<Arc<str>, Arc<str>>, client: &str, commands: &Vec<Arc<str>>) -> RedisResult<String>{
        let mut channel_guard = self.channels_state().channels_map.write()?;
        if let Some((count, client_set)) = channel_guard.get_mut(&commands[1]){
            *count -= 1;
            client_set.remove(client);
        }

        let subscriptions = client_state.get_subscriptions_mut();
        subscriptions.1.remove(&commands[1]);
        subscriptions.0 -= 1;
        let subs_count = subscriptions.0;

        Ok(format!("*3\r\n${}\r\n{}\r\n${}\r\n{}\r\n:{}\r\n", commands[0].len(), commands[0].to_lowercase(), commands[1].len(), commands[1], subs_count))
    }

}