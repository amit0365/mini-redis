use std::{collections::{HashMap, HashSet, VecDeque}, marker::PhantomData, sync::{Arc, Mutex, RwLock}, time::{Duration, Instant}};
use tokio::{sync::mpsc::{self, Receiver, Sender, error::TrySendError}, time::sleep};
use serde_json::json;

use crate::{protocol::{RedisValue, StreamValue}, utils::{collect_as_strings, encode_resp_array, encode_resp_value_array, parse_wrapback}};

#[derive(Clone)]
pub struct RedisState<K, RedisValue> {
    channels_state: ChannelState<K>,
    map_state: MapState<K, RedisValue>,
    list_state: ListState<K, RedisValue>,
    server_state: ServerState<K, RedisValue>
}

impl<K, RedisValue> RedisState<K, RedisValue> {
    pub fn channels_state(&self) -> &ChannelState<K> {
        &self.channels_state
    }

    pub fn channels_state_mut(&mut self) -> &mut ChannelState<K> {
        &mut self.channels_state
    }

    pub fn map_state(&self) -> &MapState<K, RedisValue> {
        &self.map_state
    }

    pub fn map_state_mut(&mut self) -> &mut MapState<K, RedisValue> {
        &mut self.map_state
    }

    pub fn list_state(&self) -> &ListState<K, RedisValue> {
        &self.list_state
    }

    pub fn list_state_mut(&mut self) -> &mut ListState<K, RedisValue> {
        &mut self.list_state
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
    channels_map: Arc<RwLock<HashMap<K, (usize, HashSet<String>)>>>,
    subscribers: Arc<Mutex<HashMap<K, Vec<Sender<(K, Arc<Vec<String>>)>>>>>,
}

impl<K> ChannelState<K>{
    fn new() -> Self{
        let channels_map = Arc::new(RwLock::new(HashMap::new()));
        let subscribers = Arc::new(Mutex::new(HashMap::new()));
        ChannelState { channels_map, subscribers }
    }

    pub fn channels_map(&self) -> &Arc<RwLock<HashMap<K, (usize, HashSet<String>)>>> {
        &self.channels_map
    }

    pub fn subscribers(&self) -> &Arc<Mutex<HashMap<K, Vec<Sender<(K, Arc<Vec<String>>)>>>>> {
        &self.subscribers
    }
}

#[derive(Clone)]
pub struct ServerState<K, V>{
    replication_mode: Arc<Mutex<bool>>,
    map: HashMap<K, V>
}

impl<K, V> ServerState<K, V> {
    pub fn replication_mode(&self) -> bool {
        *self.replication_mode.lock().unwrap()
    }

    pub fn set_replication_mode(&mut self, mode: bool) {
        *self.replication_mode.lock().unwrap() = mode;
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
    replica_offsets: HashMap<String, usize>,
    master_write_offset: Arc<RwLock<usize>>,
    replica_senders: Arc<Mutex<HashMap<String, Sender<Arc<String>>>>>
}

impl ReplicasState {
    pub fn new() -> Self {
        ReplicasState {
            num_connected_replicas: Arc::new(RwLock::new(0)),
            replica_offsets: HashMap::new(),
            master_write_offset: Arc::new(RwLock::new(0)),
            replica_senders: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub fn increment_master_write_offset(&mut self, n: usize){
       *self.master_write_offset.write().unwrap() += n;
    }

    pub fn num_connected_replicas(&self) -> usize {
        *self.num_connected_replicas.read().unwrap()
    }

    pub fn get_replica_offsets(&self) -> &HashMap<String, usize> {
        &self.replica_offsets
    }

    pub fn get_master_write_offset(&self) -> usize {
        *self.master_write_offset.read().unwrap()
    }

    pub fn increment_num_connected_replicas(&mut self){
       *self.num_connected_replicas.write().unwrap() += 1;
    }

    pub fn replica_senders(&self) -> &Arc<Mutex<HashMap<String, Sender<Arc<String>>>>> {
        &self.replica_senders
    }
}

impl ServerState<String, RedisValue>{
    fn new() -> Self{
        ServerState { replication_mode: Arc::new(Mutex::new(false)), map: HashMap::new() }
    }

    pub fn update(&mut self, pairs: Vec<(String, RedisValue)>){
        for (k, v) in pairs {
            self.map_mut().insert(k, v);
        }
    }
}

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
    addr: String,
    is_replica: bool,
    num_bytes_synced: usize,
    receiver: Option<Receiver<Arc<String>>>,
    _phantom: PhantomData<(K, V)>
}

impl<K, V> ReplicationState<K, V> {
    fn new() -> Self {
        ReplicationState {
            addr: String::new(),
            is_replica: false,
            num_bytes_synced: 0,
            receiver: None,
            _phantom: PhantomData,
        }
    }

    pub fn get_address(&self) -> &String {
        &self.addr
    }

    pub fn set_address(&mut self, addr: String){
        self.addr = addr;
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

    pub fn get_receiver_mut(&mut self) -> Option<&mut Receiver<Arc<String>>> {
        self.receiver.as_mut()
    }

    pub fn set_receiver(&mut self, receiver: Receiver<Arc<String>>) {
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

    pub fn get_queued_commands(&self) -> &VecDeque<Vec<V>> {
        &self.queued_commands
    }

    pub fn get_queued_commands_mut(&mut self) -> &mut VecDeque<Vec<V>> {
        &mut self.queued_commands
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

impl ClientState<String, String>{
    pub fn new() -> Self{
        ClientState {
            queued_state: QueuedState::new(),
            subscription_state: SubscriptionState::new(),
            replication_state: ReplicationState::new(),
        }
    }

    pub fn get_address_replica(&self) -> &String {
        &self.replication_state.get_address()
    }

    pub fn set_address_replica(&mut self, addr: String){
        self.replication_state.set_address(addr);
    }

    // Delegation methods for SubscriptionState
    pub fn is_subscribe_mode(&self) -> bool {
        self.subscription_state.is_subscribe_mode()
    }

    pub fn set_subscribe_mode(&mut self, mode: bool) {
        self.subscription_state.set_subscribe_mode(mode);
    }

    pub fn get_subscriptions(&self) -> &(usize, HashSet<String>) {
        self.subscription_state.get_map()
    }

    pub fn get_subscriptions_mut(&mut self) -> &mut (usize, HashSet<String>) {
        self.subscription_state.get_map_mut()
    }

    pub fn get_sub_receiver_mut(&mut self) -> Option<&mut Receiver<(String, Arc<Vec<String>>)>> {
        self.subscription_state.get_receiver_mut()
    }

    pub fn get_sub_sender(&self) -> &Option<Sender<(String, Arc<Vec<String>>)>> {
        self.subscription_state.get_sender()
    }

    pub fn get_replica_receiver_mut(&mut self) -> Option<&mut Receiver<Arc<String>>> {
        self.replication_state.get_receiver_mut()
    }

    pub fn set_channel(&mut self, sender: Sender<(String, Arc<Vec<String>>)>, receiver: Receiver<(String, Arc<Vec<String>>)>) {
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

    pub fn get_queued_commands(&self) -> &VecDeque<Vec<String>> {
        self.queued_state.get_queued_commands()
    }

    pub fn get_queued_commands_mut(&mut self) -> &mut VecDeque<Vec<String>> {
        self.queued_state.get_queued_commands_mut()
    }

    pub fn push_command(&mut self, command: Vec<String>) {
        self.queued_state.push_command(command);
    }

    pub fn pop_command(&mut self) -> Option<Vec<String>> {
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

    pub fn set_replica_receiver(&mut self, receiver: Receiver<Arc<String>>){
        self.replication_state.set_receiver(receiver);
    }
}

#[derive(Clone)]
pub struct ListState<K, RedisValue>{
    list: Arc<Mutex<HashMap<K, VecDeque<RedisValue>>>>,
    waiters: Arc<Mutex<HashMap<K, VecDeque<Sender<(K, RedisValue)>>>>>,
}

impl<K> ListState<K, RedisValue>{
    fn new() -> Self{
        let list = Arc::new(Mutex::new(HashMap::new()));
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        ListState { list, waiters }
    }

    pub fn list(&self) -> &Arc<Mutex<HashMap<K, VecDeque<RedisValue>>>> {
        &self.list
    }

    pub fn waiters(&self) -> &Arc<Mutex<HashMap<K, VecDeque<Sender<(K, RedisValue)>>>>> {
        &self.waiters
    }
}

#[derive(Clone)]
pub struct MapState<K, RedisValue>{
    map: Arc<RwLock<HashMap<K, RedisValue>>>,
    waiters: Arc<Mutex<HashMap<K, VecDeque<Sender<(K, RedisValue)>>>>>,
}

impl<K> MapState<K, RedisValue>{
    fn new() -> Self{
        let map = Arc::new(RwLock::new(HashMap::new()));
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        MapState { map, waiters }
    }

    pub fn map(&self) -> &Arc<RwLock<HashMap<K, RedisValue>>> {
        &self.map
    }

    pub fn waiters(&self) -> &Arc<Mutex<HashMap<K, VecDeque<Sender<(K, RedisValue)>>>>> {
        &self.waiters
    }
}

impl RedisState<String, RedisValue>{
    pub fn new() -> Self{
        let channels_state = ChannelState::new();
        let map_state = MapState::new();
        let list_state = ListState::new();
        let server_state = ServerState::new();
        RedisState { channels_state, map_state, list_state, server_state }
    }

    pub fn psync(&self) -> String {
        let repl_id = self.server_state().map().get("master_replid").unwrap();
        let offset = self.server_state().map().get("master_repl_offset").unwrap();
        let fullresync_response = format!("+FULLRESYNC {} {}\r\n", repl_id, offset);
        fullresync_response
    }

    pub fn set(&mut self, commands: &Vec<String>) -> String {
        let key = commands[1].clone();
        let value = commands[2].clone();
        match commands.iter().skip(3).next() {
            Some(str) => {
                match str.to_uppercase().as_str() {
                    "PX" => {
                        let timeout = Instant::now() + Duration::from_millis(commands[4].parse::<u64>().unwrap());
                        self.map_state().map.write().unwrap().insert(key, RedisValue::StringWithTimeout((value, timeout)));
                    }
                    "EX" => {
                        let timeout = Instant::now() + Duration::from_secs(commands[4].parse::<u64>().unwrap());
                        self.map_state().map.write().unwrap().insert(key, RedisValue::StringWithTimeout((value, timeout)));
                    }
                    _ => (),
                }
            }
            None => {
                let redis_val = match value.parse::<u64>(){
                    Ok(num) => RedisValue::Number(num),
                    Err(_) => RedisValue::String(value),
                };

                self.map_state().map.write().unwrap().insert(key, redis_val);
            }
        }

        format!("+OK\r\n")
    }

    pub fn get(&mut self, commands: &Vec<String>) -> String {
        let value = self.map_state().map.read().unwrap().get(&commands[1]).cloned();
        if let Some(value) = value {
            match value {
                RedisValue::StringWithTimeout((value, timeout)) => {
                    if Instant::now() < timeout {
                        format!("${}\r\n{}\r\n", value.len(), value)
                    } else {
                        format!("$-1\r\n")
                    }
                }
                RedisValue::String(val) => {
                    format!("${}\r\n{}\r\n", val.len(), val)
                }
                RedisValue::Number(val) => {
                    format!("${}\r\n{}\r\n", val.to_string().len(), val)
                }
                _ => format!("$-1\r\n") // fix error handling
            }
        } else { format!("$-1\r\n")}
    } 

    pub fn rpush(&mut self, commands: &Vec<String>) -> String {
        let mut count = String::new();
        let key = &commands[1];

        {
            let mut list_guard = self.list_state().list.lock().unwrap();
            let items = commands.iter().skip(2).map(|v| RedisValue::String(v.to_string())).collect::<Vec<RedisValue>>();
            list_guard.entry(key.clone())
            .or_insert(VecDeque::new())
            .extend(items);
            count = list_guard.get(key).unwrap().len().to_string();
        }
        
        let mut waiters_guard = self.list_state().waiters.lock().unwrap();
        if let Some(waiting_queue) = waiters_guard.get_mut(key){
            while let Some(sender) = waiting_queue.pop_front(){
                let mut list_guard = self.list_state().list.lock().unwrap();
                if let Some(deque) = list_guard.get_mut(key){
                    if let Some(value) = deque.pop_front(){
                        drop(list_guard); // Release lock before sending
                        match sender.try_send((key.clone(), value)){
                            Ok(_) => break,
                            Err(TrySendError::Full(_)) => return format!("ERR_TOO_MANY_WAITERS"),
                            Err(TrySendError::Closed(_)) => continue,
                        }
                    }
                }
            }
        }

        format!(":{}\r\n", count)
    } 

    pub fn lpush(&mut self, commands: &Vec<String>) -> String {
        let mut list_guard = self.list_state().list.lock().unwrap();
        let items = commands.iter().skip(2).map(|v| RedisValue::String(v.to_string())).collect::<Vec<RedisValue>>();
        for item in items.iter() {
            list_guard.entry(commands[1].clone())
            .or_insert(VecDeque::new())
            .push_front(item.clone());
        }
        
        let count = list_guard.get(&commands[1]).unwrap().len();
        format!(":{}\r\n", count.to_string())
    } 

    pub fn llen(&self, commands: &Vec<String>) -> String {
        let list_guard = self.list_state().list.lock().unwrap();
        let len = match list_guard.get(&commands[1]){
            Some(list) => list.len().to_string(),
            None => 0.to_string()
        };

        format!(":{}\r\n", len)
    } 

    pub fn lpop(&mut self, commands: &Vec<String>) -> String {
        let mut list_guard = self.list_state().list.lock().unwrap();
        match list_guard.get_mut(&commands[1]){
            Some(list) => {
                match commands.iter().skip(2).next(){
                    Some(n) => {
                        let mut popped_list = Vec::new();
                        let len = n.parse::<usize>().unwrap(); //remove unwrap
                        for _ in 0..len{
                            match list.pop_front(){
                                Some(popped) => {
                                    if let Some(val) = popped.as_string(){
                                        popped_list.push(val.clone());
                                    }
                                }
                                None => (),
                            }
                        }

                        encode_resp_array(&popped_list)
                    },

                    None => {
                        match list.pop_front(){
                            Some(popped) => {
                                if let Some(val) = popped.as_string(){
                                    format!("${}\r\n{}\r\n", val.len(), val)
                                } else {format!("$-1\r\n")} // fix this
                            }
                            None => format!("$-1\r\n"),
                        }
                    },
                }
            },

            None => format!("$-1\r\n")
        }
    } 

    pub async fn blpop(&mut self, commands: &Vec<String>) -> String {
        let key = &commands[1];

        {
            let mut list_guard = self.list_state().list.lock().unwrap();
            if let Some(list) = list_guard.get_mut(key){
                if let Some(data) = list.pop_front(){
                    if let Some(val) = data.as_string(){
                        return encode_resp_array(&vec![key.clone(), val.clone()])
                    }
                }
            }
        }

        let mut receiver = {
            let mut waiters_guard = self.list_state().waiters.lock().unwrap();
            let queue = waiters_guard.entry(key.clone()).or_insert(VecDeque::new());
            if queue.len() > 10000 {
                return format!("ERR_TOO_MANY_BLPOP_WAITERS_FOR_THE_KEY")
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

        let timeout: f64 = commands.last().unwrap().parse().unwrap(); // yo check this
        if timeout == 0.0 {
            match receiver.recv().await{
                Some((key, value)) => {
                    if let Some(val) = value.as_string(){
                        encode_resp_array(&vec![key, val.clone()])
                    } else {format!("$\r\n")} //fix this
                }
                None => format!("$\r\n"), //todo fix this
            }
        } else {
            tokio::select! {
                result = receiver.recv() => {
                    if let Some((key, value)) = result{
                        if let Some(val) = value.as_string(){
                            encode_resp_array(&vec![key, val.clone()])
                        } else {format!("$\r\n")} //fix this
                    } else {format!("*-1\r\n")} // none so disconnected or dropped
                }

                _ = sleep(Duration::from_secs_f64(timeout)) => {
                    format!("*-1\r\n")
                }
            }
        }
    }
    
    pub fn lrange(&self, key: &String, start: &String, stop: &String) -> String {
        let list_guard = self.list_state().list.lock().unwrap();
        let array = match list_guard.get(key){
            Some(vec) => {
                let start = parse_wrapback(start.parse::<i64>().unwrap(), vec.len());
                let stop = parse_wrapback(stop.parse::<i64>().unwrap(), vec.len());

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

        encode_resp_array(&collect_as_strings(array))
    } 

    pub fn type_command(&self, commands: &Vec<String>) -> String {
        let map_guard = self.map_state().map.read().unwrap();
        let response = match map_guard.get(&commands[1]){
            Some(val) => {
                match val{
                    RedisValue::String(_) => "string".to_string(),
                    RedisValue::StringWithTimeout(_) => "string".to_string(),
                    RedisValue::Stream(_) => "stream".to_string(),
                    RedisValue::Number(_) => "number".to_string(),
                    RedisValue::Commands(_) => "commands".to_string(),
                }
            },
            None => "none".to_string()
        };

        format!("+{}\r\n", response)
    } 

    pub fn xadd(&self, commands: &Vec<String>) -> String {
        let key = &commands[1];
        let mut result = String::new();
        let pairs_grouped = commands[3..].chunks_exact(2).map(|chunk| (chunk[0].clone(), chunk[1].clone())).collect::<Vec<_>>();

        let has_waiters = {
            let map_waiters_guard = self.map_state().waiters.lock().unwrap();
            map_waiters_guard.get(key).map_or(false, |q| !q.is_empty())
        };

        let pairs_for_waiter = if has_waiters {
            Some(pairs_grouped.clone())
        } else {
            None  
        };

        {
            let mut map_guard = self.map_state().map.write().unwrap();
            result = map_guard.entry(key.clone())
            .or_insert(RedisValue::Stream(StreamValue::new()))
            .update_stream(&commands[2], pairs_grouped);
        }

        if let Some(pairs) = pairs_for_waiter {
            let mut map_waiters_guard = self.map_state().waiters.lock().unwrap();
            if let Some(waiters_queue) = map_waiters_guard.get_mut(key){
                let stream_value = StreamValue::new_blocked(Arc::from(commands[2].as_str()), &pairs);
                while let Some(waiter) = waiters_queue.pop_front(){
                    match waiter.try_send((key.clone(), RedisValue::Stream(stream_value.clone()))){
                        Ok(_) => break,
                        Err(TrySendError::Full(_)) => return format!("ERR_TOO_MANY_WAITERS"),
                        Err(TrySendError::Closed(_)) => continue,
                    }
                }
            }
        }

        result
    } 

    pub fn xrange(&self, commands: &Vec<String>) -> String {
        let mut encoded_array = String::new();
        let values = self.map_state().map.read().unwrap()
        .get(&commands[1])
        .unwrap() // no case for nil
        .get_stream_range(&commands[2], Some(&commands[3]));
        if !values.is_empty() {
            encode_resp_value_array(&mut encoded_array, &values);
            encoded_array
        } else { format!("ERR_NOT_SUPPORTED") } // fix error handling
    } 

    pub async fn xread(&self, commands: &Vec<String>) -> String {
        match commands[1].as_str(){
            "streams" => {
                let key_tokens = commands.iter().skip(2)
                .filter(|token| self.map_state().map.read().unwrap().get(token.as_str()).is_some())
                .collect::<Vec<_>>();
        
                let id_tokens = commands.iter().skip(2 + key_tokens.len()).collect::<Vec<_>>();
                let mut encoded_array = String::new();
                let mut key_entries = Vec::new();
        
                for (key, id) in key_tokens.iter().zip(id_tokens){
                    let values = self.map_state().map.read().unwrap()
                    .get(*key)
                    .unwrap() // no case for nil
                    .get_stream_range(id, None);
                    if !values.is_empty() { key_entries.push(json!([key, values]));}
                }
        
                if !key_entries.is_empty() {
                    encode_resp_value_array(&mut encoded_array, &key_entries);
                    encoded_array
                } else { format!("ERR_NOT_SUPPORTED") } // fix error handling
            },

            "block" => {
                let timeout = &commands[2].parse::<u64>().unwrap();
                let key = &commands[4];

                let mut receiver = {
                    let mut waiters_guard = self.map_state().waiters.lock().unwrap();
                    let queue = waiters_guard.entry(key.to_owned()).or_insert(VecDeque::new());
                    if queue.len() > 10000{
                        return format!("ERR_TOO_MANY_XREAD_WAITERS_FOR_THE_KEY")
                    }

                    let (sender, receiver) = mpsc::channel(1);
                    queue.push_back(sender);
                    receiver
                };

                if *timeout == 0 {
                    match receiver.recv().await{
                        Some((key, redis_val)) => {
                            if let Some(value_array) = redis_val.get_blocked_result(){
                                let mut encoded_array = String::new();
                                encode_resp_value_array(&mut encoded_array, &vec![json!([key, value_array])]);
                                return encoded_array
                            }
                            format!("*-1\r\n")
                        },
                        None => format!("*-1\r\n")
                    }
                } else {
                    tokio::select! {
                        result = receiver.recv() => {
                            if let Some((key, redis_val)) = result {
                                if let Some(value_array) = redis_val.get_blocked_result(){
                                    let mut encoded_array = String::new();
                                    encode_resp_value_array(&mut encoded_array, &vec![json!([key, value_array])]);
                                    return encoded_array
                                }
                                format!("*-1\r\n")  
                            } else {format!("*-1\r\n")}
                        },
    
                        _ = sleep(Duration::from_millis(*timeout)) => {
                            format!("*-1\r\n")
                        },
                    }
                }
            },

            _ => format!("ERR_NOT_SUPPORTED")
        }
    }

    pub fn incr(&self, commands: &Vec<String>) -> String {
        let mut map_guard = self.map_state().map.write().unwrap();
        let val = map_guard.entry(commands[1].to_owned()).or_insert(RedisValue::Number(0));
        match val{
            RedisValue::Number(n) => {
                *n += 1;
                format!(":{}\r\n", n)
            },
            _ => format!("-ERR value is not an integer or out of range\r\n"),
        }
    } 

    pub fn multi(&self, client_state: &mut ClientState<String, String>) -> String {
        client_state.set_multi_queue_mode(true);
        format!("+OK\r\n")
    } 

    pub fn info(&self, commands: &Vec<String>) -> String {
        match commands[1].to_uppercase().as_str(){
            "REPLICATION" => {
                let mut lines = Vec::new();
                for (key, value) in self.server_state().map().iter() {
                    lines.push(format!("{}:{}", key, value));
                }
                let content = lines.join("\r\n");
                format!("${}\r\n{}\r\n", content.len(), content)
            }
            _ => format!("NOT_SUPPORTED"),
        }
    } 

    pub fn subscribe(&mut self, client_state: &mut ClientState<String, String>, client: &String, commands: &Vec<String>) -> String{
        client_state.set_subscribe_mode(true);
        let subs_count = if client_state.get_subscriptions().1.contains(&commands[1]){
            client_state.get_subscriptions().0
        } else {
            let mut channel_guard = self.channels_state().channels_map.write().unwrap();
            let (count, client_set) = channel_guard.entry(commands[1].to_owned()).or_insert((0, HashSet::new()));
            *count += 1;
            client_set.insert(client.to_owned());

            let subscriptions = client_state.get_subscriptions_mut();
            subscriptions.1.insert(commands[1].to_owned());
            subscriptions.0 += 1;
            subscriptions.0
        };

        format!("*3\r\n${}\r\n{}\r\n${}\r\n{}\r\n:{}\r\n", commands[0].len(), commands[0].to_lowercase(), commands[1].len(), commands[1], subs_count)
    }

    pub async fn handle_subscriber(&self, client_state: &mut ClientState<String, String>, commands: &Vec<String>){
        if client_state.has_receiver() {
            let (sender, receiver) = mpsc::channel(1000);
            client_state.set_channel(sender, receiver);
        }

        if let Some(sender) = client_state.get_sub_sender() {
            let mut subs_guard = self.channels_state().subscribers.lock().unwrap();
            subs_guard.entry(commands[1].to_owned()).or_insert(Vec::new()).push(sender.clone());
        }
    }

    pub fn publish(&self, commands: &Vec<String>) -> String{
        let subs = self.channels_state().channels_map.read().unwrap().get(&commands[1]).unwrap().0;
        let channel_name = &commands[1];
        let messages = Arc::new(commands.iter().skip(2).cloned().collect::<Vec<_>>());
        let mut subs_guard = self.channels_state().subscribers.lock().unwrap();
        if let Some(subs) = subs_guard.get_mut(&commands[1]){
            subs.retain(|sender| 
                match sender.try_send((channel_name.to_owned(), messages.clone())){
                    Ok(_) => true,
                    Err(TrySendError::Full(_)) => true,
                    Err(TrySendError::Closed(_)) => false,
            })
        }

        format!(":{}\r\n", subs)
    }

    pub fn unsubscribe(&self, client_state: &mut ClientState<String, String>, client: &String, commands: &Vec<String>) -> String{
        let mut channel_guard = self.channels_state().channels_map.write().unwrap();
        if let Some((count, client_set)) = channel_guard.get_mut(&commands[1]){
            *count -= 1;
            client_set.remove(client);
        }

        let subscriptions = client_state.get_subscriptions_mut();
        subscriptions.1.remove(&commands[1]);
        subscriptions.0 -= 1;
        let subs_count = subscriptions.0;

        format!("*3\r\n${}\r\n{}\r\n${}\r\n{}\r\n:{}\r\n", commands[0].len(), commands[0].to_lowercase(), commands[1].len(), commands[1], subs_count)
    }

}