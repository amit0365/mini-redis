Critical Clone Issues 🔴

1. src/protocol/value.rs ✅ FIXED
Lines 30-31, 38-39: String cloning when building pair vectors
Changed signature to accept Vec<(String, String)> directly instead of &Vec<String>
- new_blocked() now takes &Vec<(String, String)> and uses to_vec() once
- insert() now takes Vec<(String, String)> by value (ownership transfer)
Impact: Reduced from 2N clones per operation to 0-1 depending on context

Line 131: Stream read optimization
Changed from cloning strings to using string slices in flattened array
- Uses .as_str() instead of .clone() when building JSON response
Impact: Eliminates 2N heap allocations during XRANGE/XREAD responses

Lines 32, 41, 157, 208, 235: ID string cloning - ✅ FIXED with Arc<str>
Implementation:
- Changed StreamValue to use Arc<str> for all ID fields (last_id, BTreeMap keys, waiters_value)
- new_blocked() now takes Arc<str> directly (no clone needed, just refcount increment)
- insert() takes Arc<str> by value (moves Arc instead of cloning String)
- update_stream() builds String once, converts to Arc<str>, then shares it across all uses
- Added custom Serialize impl to handle Arc<str> serialization (converts to &str)
- Call site in state.rs:759 converts String to Arc once: Arc::from(commands[2].as_str())

Impact:
- Before: 3-5 String clones per XADD (10-30 bytes heap allocation each)
- After: 3-5 Arc clones per XADD (8 bytes pointer + atomic refcount increment)
- Memory: IDs stored once, shared via Arc across BTreeMap, last_id, and waiters
- Performance: ~60-80% reduction in allocations for ID management


2. src/protocol/state.rs
Line 144: Cloning values during bulk update - ✅ FIXED
Implementation:
- Changed update() signature from &Vec<(String, RedisValue)> to Vec<(String, RedisValue)>
- Replaced iter().map() chain with simple for loop that moves values
- Updated call site in replication.rs:145 to pass ownership instead of reference

Impact:
- Before: Iterator chain that clones both keys (String) and values (RedisValue with nested BTreeMap/HashMap)
- After: Direct ownership transfer - no clones at all
- Benefit: Eliminates 3 String clones + 3 deep RedisValue clones during master state initialization

Lines 418, 422: SET command clones
insert(commands[1].clone(), RedisValue::StringWithTimeout((commands[2].clone(), timeout)))
Problem: Double cloning of command strings
Fix: Use slice indexing or swap/take ownership

Lines 469, 482, 499, 501, 565, 573, 588, 598, 658, 666: Key and value cloning in list/stream operations
list_guard.entry(key.clone())
sender.try_send((key.clone(), value))
Problem: Keys cloned multiple times in the same function
Impact: High-frequency operations (RPUSH, BLPOP) suffer most

Line 530: Cloning popped values
popped_list.push(val.clone())
Problem: Values already being removed from deque, but then cloned
Fix: Use drain() or move values out

Line 820, 831: Sender and message cloning in pub/sub
.push(sender.clone())
sender.try_send((channel_name.to_owned(), messages.clone()))
Problem: messages is a Vec<String> being cloned for every subscriber
Impact: With 100 subscribers, 100x heap allocation for same message


3. src/main.rs

Lines 215-216: State cloning per connection
let mut local_state = state.clone();
let mut local_replicas_state = replicas_state.clone();
Problem: MOST CRITICAL - Entire state cloned for every TCP connection
Impact: With concurrent connections, massive memory duplication. The RedisState contains Arc<RwLock<HashMap>> which is cheap to clone, BUT the comment shows you're cloning the wrong thing
Fix: These are already using Arc internally, so cloning is actually cheap here (false alarm, but confusing)

Line 109: Replica senders cloning
replica_senders_guard.clone()
Problem: Cloning entire Vec of senders
Fix: Iterate and clone only senders that need messages, or use Arc-wrapped vec

Line 224: Connection count Arc clone
let cc = connection_count.clone()
Assessment: This is fine (Arc clone is cheap)


4. src/utils.rs
Line 45: Cloning entire encoded array
encoded_array.clone()
Problem: Returns clone of accumulated string buffer
Fix: Take ownership of the parameter instead of &mut
Optimization Priorities 📊
High Impact (Do these first):
Line 831 in state.rs - Pub/Sub message broadcast cloning
Use Arc<Vec<String>> for messages to share across subscribers
Lines 30-31, 38-39 in value.rs - Stream pair cloning
Change signature to accept Vec<(String, String)> directly instead of flattened vec
Line 118 in state.rs - Bulk update cloning
Change to update(&mut self, pairs: Vec<(String, RedisValue)>) and use drain()
Line 109 in main.rs - Replica senders vec clone
Keep vec in Arc and clone individual senders during iteration
Line 501 in state.rs - LPUSH cloning in loop
Move items instead of cloning them
Medium Impact:
Key cloning in entry patterns (lines 469, 499, 573, 658)
Use to_owned() on the command slice once, store in variable
StreamValue ID management (lines 157, 208, 235)
Use String::new() and mutation instead of cloning
Low Impact (micro-optimizations):
Line 45 utils.rs - Return by move instead of clone
Command string clones in SET/GET paths