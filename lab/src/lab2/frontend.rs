use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::cmp::{min, Ordering};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tribbler::err::TribResult;
use tribbler::storage;
use tribbler::storage::BinStorage;
use tribbler::trib::{self, MAX_FOLLOWING};
use tribbler::{
    err::TribblerError,
    trib::{is_valid_username, Trib, MAX_TRIB_FETCH, MAX_TRIB_LEN, MIN_LIST_USER},
};

pub static BIN_SIGN_UP: &str = "SIGNUP";
pub static KEY_SIGNED_UP_USERS: &str = "users";
pub static KEY_TRIBS: &str = "tribs";
pub static KEY_FOLLOWING_LOG: &str = "following_log";

pub struct FrontEnd {
    pub bin_storage: Box<dyn BinStorage>,
    pub cached_users: Mutex<Vec<String>>,
}
/// A [Trib] type with an additional sequence number
#[derive(Debug, Clone)]
struct SeqTrib {
    logical_clock: u64,
    physical_clock: u64,
    user_name: String,
    message: String,
    trib: Arc<Trib>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
struct FollowUnfollowLog {
    is_follow: bool,
    id: u64,
    whom: String,
}
#[derive(Debug, Clone)]
struct LogResult {
    valid: bool,
    log: FollowUnfollowLog,
}
impl Ord for SeqTrib {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.logical_clock.cmp(&other.logical_clock) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => match self.physical_clock.cmp(&other.physical_clock) {
                Ordering::Greater => Ordering::Greater,
                Ordering::Less => Ordering::Less,
                Ordering::Equal => match self.user_name.cmp(&other.user_name) {
                    Ordering::Greater => Ordering::Greater,
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => self.message.cmp(&other.message),
                },
            },
        }
    }
}

impl Eq for SeqTrib {}

impl PartialOrd for SeqTrib {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.logical_clock.partial_cmp(&other.logical_clock) {
            None => None,
            Some(Ordering::Greater) => Some(Ordering::Greater),
            Some(Ordering::Less) => Some(Ordering::Less),
            Some(Ordering::Equal) => match self.physical_clock.partial_cmp(&other.physical_clock) {
                None => None,
                Some(Ordering::Greater) => Some(Ordering::Greater),
                Some(Ordering::Less) => Some(Ordering::Less),
                Some(Ordering::Equal) => match self.user_name.partial_cmp(&other.user_name) {
                    None => None,
                    Some(Ordering::Greater) => Some(Ordering::Greater),
                    Some(Ordering::Less) => Some(Ordering::Less),
                    Some(Ordering::Equal) => self.message.partial_cmp(&other.message),
                },
            },
        }
    }
}

impl PartialEq for SeqTrib {
    fn eq(&self, other: &Self) -> bool {
        self.logical_clock == other.logical_clock
            && self.physical_clock == other.physical_clock
            && self.user_name == other.user_name
            && self.message == other.message
    }
}

#[async_trait]
impl trib::Server for FrontEnd {
    /// Creates a user.
    /// Returns error when the username is invalid;
    /// returns error when the user already exists.
    /// Concurrent sign ups on the same user might both succeed with no error.
    async fn sign_up(&self, user: &str) -> TribResult<()> {
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }
        let special_sign_up_bin = match self.bin_storage.bin(BIN_SIGN_UP).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(registered_users_list) =
            match special_sign_up_bin.list_get(KEY_SIGNED_UP_USERS).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
        if registered_users_list.contains(&user.to_string()) {
            return Err(Box::new(TribblerError::UsernameTaken(user.to_string())));
        }
        match special_sign_up_bin
            .list_append(&storage::KeyValue {
                key: "users".to_string(),
                value: user.to_string(),
            })
            .await
        {
            Ok(_v) => Ok(()),
            Err(e) => Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        }
    }

    /// List 20 registered users.  When there are less than 20 users that
    /// signed up the service, all of them needs to be listed.  When there
    /// are more than 20 users that signed up the service, an arbitrary set
    /// of at lest 20 of them needs to be listed.
    /// The result should be sorted in alphabetical order.
    async fn list_users(&self) -> TribResult<Vec<String>> {
        let res_users_list: Vec<String>;
        let cached_users_list = (*self.cached_users.lock().unwrap()).clone();
        if cached_users_list.len() < 20 {
            let special_sign_up_bin = match self.bin_storage.bin(BIN_SIGN_UP).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
            let storage::List(registered_users_list) =
                match special_sign_up_bin.list_get(KEY_SIGNED_UP_USERS).await {
                    Ok(v) => v,
                    Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
                };
            let mut k: Vec<&str> = registered_users_list.iter().map(|s| &**s).collect();
            k.sort();
            let sorted = k[..min(MIN_LIST_USER, k.len())].to_vec();
            res_users_list = sorted
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>();
            let mut mut_cached_users = self.cached_users.lock().unwrap();
            *mut_cached_users = res_users_list.clone();
            std::mem::drop(mut_cached_users);
        } else {
            res_users_list = cached_users_list.clone();
        }
        Ok(res_users_list)
    }

    /// Post a tribble.  The clock is the maximum clock value this user has
    /// seen so far by reading tribbles or clock sync.
    /// Returns error when who does not exist;
    /// returns error when post is too long.
    async fn post(&self, who: &str, post: &str, clock: u64) -> TribResult<()> {
        if post.len() > MAX_TRIB_LEN {
            return Err(Box::new(TribblerError::TribTooLong));
        }
        if !is_valid_username(who) {
            return Err(Box::new(TribblerError::InvalidUsername(who.to_string())));
        }
        let special_sign_up_bin = match self.bin_storage.bin(BIN_SIGN_UP).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(registered_users_list) =
            match special_sign_up_bin.list_get(KEY_SIGNED_UP_USERS).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
        if !registered_users_list.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        let user_bin = match self.bin_storage.bin(who).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let logical_clk = match user_bin.clock(clock).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let trib = Trib {
            user: who.to_string(),
            message: post.to_string(),
            time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs(),
            clock: logical_clk,
        };
        let serialized_trib = serde_json::to_string(&trib).unwrap();
        match user_bin
            .list_append(&storage::KeyValue {
                key: KEY_TRIBS.to_string(),
                value: serialized_trib,
            })
            .await
        {
            Ok(_v) => Ok(()),
            Err(e) => Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        }
    }

    /// List the tribs that a particular user posted.
    /// Returns error when user has not signed up.
    async fn tribs(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }
        let special_sign_up_bin = match self.bin_storage.bin(BIN_SIGN_UP).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(registered_users_list) =
            match special_sign_up_bin.list_get(KEY_SIGNED_UP_USERS).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
        if !registered_users_list.contains(&user.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }
        let user_bin = match self.bin_storage.bin(user).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(trib_list) = match user_bin.list_get(KEY_TRIBS).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let deserialized_trib_list: Vec<Trib> = trib_list
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<Trib>>();
        //sort the trib  list and fetch recent MAX_FETCH tribs
        let mut seq_trib_list: Vec<SeqTrib> = deserialized_trib_list
            .iter()
            .map(|x| SeqTrib {
                logical_clock: x.clock,
                physical_clock: x.time,
                user_name: x.user.to_string(),
                message: x.message.to_string(),
                trib: Arc::<Trib>::new(x.clone()),
            })
            .collect::<Vec<SeqTrib>>();

        seq_trib_list.sort();

        let ntrib = seq_trib_list.len();
        let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => ntrib - MAX_TRIB_FETCH,
            _ => 0,
        };

        let res_list: Vec<Arc<Trib>> = seq_trib_list[start..]
            .to_vec()
            .iter()
            .map(|x| x.trib.clone())
            .collect::<Vec<Arc<Trib>>>();
        if ntrib > MAX_TRIB_FETCH {
            let to_be_removed_tribs: Vec<String> = seq_trib_list[..start]
                .to_vec()
                .iter()
                .map(|x| serde_json::to_string(&(x.trib.clone())).unwrap())
                .collect::<Vec<String>>();

            for trib in to_be_removed_tribs.iter() {
                let _ = match user_bin
                    .list_remove(&storage::KeyValue {
                        key: KEY_TRIBS.to_string(),
                        value: trib.to_string(),
                    })
                    .await
                {
                    Ok(_v) => {}
                    Err(_e) => {} // we are not concerned if this errors; just cleanup failed
                };
            }
        }
        Ok(res_list)
    }

    /// Follow someone's timeline.
    /// Returns error when who == whom;
    /// returns error when who is already following whom;
    /// returns error when who is trying to following
    /// more than trib.MaxFollowing users.
    /// returns error when who or whom has not signed up.
    /// Concurrent follows might both succeed without error.
    /// The count of following users might exceed trib.MaxFollowing=2000,
    /// if and only if the 2000'th user is generated by concurrent Follow()
    /// calls.
    async fn follow(&self, who: &str, whom: &str) -> TribResult<()> {
        // return error if who == whom
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        if !is_valid_username(who) {
            return Err(Box::new(TribblerError::InvalidUsername(who.to_string())));
        }
        if !is_valid_username(whom) {
            return Err(Box::new(TribblerError::InvalidUsername(whom.to_string())));
        }
        // check if who and whom have signed up
        let special_sign_up_bin = match self.bin_storage.bin(BIN_SIGN_UP).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(registered_users_list) =
            match special_sign_up_bin.list_get(KEY_SIGNED_UP_USERS).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
        if !registered_users_list.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        if !registered_users_list.contains(&whom.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }
        //generated a log
        let user_bin = match self.bin_storage.bin(who).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let logical_clk = match user_bin.clock(0_u64).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let log = FollowUnfollowLog {
            is_follow: true, // true: follow(), false: unfollow() ==> defines operation
            id: logical_clk,
            whom: whom.to_string(),
        };
        let serialized_log = serde_json::to_string(&log).unwrap();
        // append log
        let _ = match user_bin
            .list_append(&storage::KeyValue {
                key: KEY_FOLLOWING_LOG.to_string(),
                value: serialized_log,
            })
            .await
        {
            Ok(_v) => {}
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        // fetch log
        let storage::List(fetched_log) = match user_bin.list_get(KEY_FOLLOWING_LOG).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let deserialized_log: Vec<FollowUnfollowLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<FollowUnfollowLog>>();
        let mut res_deserialized_log: Vec<LogResult> = deserialized_log
            .iter()
            .map(|x| LogResult {
                valid: true,
                log: x.clone(),
            })
            .collect::<Vec<LogResult>>();

        // parse log and identify (1) whether follow should be successful; (2) number of following users including this follow() result
        let mut follow_result = false;
        let mut found_log_entry = false;
        let mut following: HashSet<String> = HashSet::new();
        for res_log in res_deserialized_log.iter_mut() {
            if res_log.log.is_follow {
                if following.contains(&res_log.log.whom) {
                    res_log.valid = false
                } else {
                    following.insert(res_log.log.whom.clone());
                }
            } else {
                if following.contains(&res_log.log.whom) {
                    following.remove(&res_log.log.whom.to_string());
                } else {
                    res_log.valid = false;
                }
            }
        }
        let following_count = following.len();
        if following_count > MAX_FOLLOWING {
            return Err(Box::new(TribblerError::FollowingTooMany));
        }
        for res_log in res_deserialized_log.iter() {
            if res_log.log.whom == whom && res_log.log.id == logical_clk {
                follow_result = res_log.valid;
                found_log_entry = true;
            }
        }
        if !(found_log_entry && follow_result) {
            return Err(Box::new(TribblerError::AlreadyFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }
        // // remove the invalid logs in the backend
        // let to_be_removed_logs: Vec<String> = res_deserialized_log
        //     .iter()
        //     .filter(|x| x.valid == false)
        //     .cloned()
        //     .collect::<Vec<LogResult>>()
        //     .iter()
        //     .map(|x| serde_json::to_string(&x.log).unwrap())
        //     .collect::<Vec<String>>();

        // for log in to_be_removed_logs.iter() {
        //     let _ = match user_bin
        //         .list_remove(&storage::KeyValue {
        //             key: KEY_FOLLOWING_LOG.to_string(),
        //             value: log.to_string(),
        //         })
        //         .await
        //     {
        //         Ok(_v) => {}
        //         Err(_e) => {} // we are not concerned if this errors; just cleanup failed
        //     };
        // }
        // no possible errors, return result to client
        Ok(())
    }

    /// Unfollow someone's timeline.
    /// Returns error when who == whom.
    /// returns error when who is not following whom;
    /// returns error when who or whom has not signed up.
    async fn unfollow(&self, who: &str, whom: &str) -> TribResult<()> {
        // return error if who == whom
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        if !is_valid_username(who) {
            return Err(Box::new(TribblerError::InvalidUsername(who.to_string())));
        }
        if !is_valid_username(whom) {
            return Err(Box::new(TribblerError::InvalidUsername(whom.to_string())));
        }

        // check if who and whom have signed up
        let special_sign_up_bin = match self.bin_storage.bin(BIN_SIGN_UP).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(registered_users_list) =
            match special_sign_up_bin.list_get(KEY_SIGNED_UP_USERS).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
        if !registered_users_list.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        if !registered_users_list.contains(&whom.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }
        //generated a log
        let user_bin = match self.bin_storage.bin(who).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let logical_clk = match user_bin.clock(0_u64).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let log = FollowUnfollowLog {
            is_follow: false, // true: follow(), false: unfollow() ==> defines operation
            id: logical_clk,
            whom: whom.to_string(),
        };
        let serialized_log = serde_json::to_string(&log).unwrap();
        // append log
        let _ = match user_bin
            .list_append(&storage::KeyValue {
                key: KEY_FOLLOWING_LOG.to_string(),
                value: serialized_log,
            })
            .await
        {
            Ok(_v) => {}
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        // fetch log
        let storage::List(fetched_log) = match user_bin.list_get(KEY_FOLLOWING_LOG).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let deserialized_log: Vec<FollowUnfollowLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<FollowUnfollowLog>>();
        let mut res_deserialized_log: Vec<LogResult> = deserialized_log
            .iter()
            .map(|x| LogResult {
                valid: true,
                log: x.clone(),
            })
            .collect::<Vec<LogResult>>();

        // parse log and identify (1) whether follow should be successful; (2) number of following users including this follow() result
        let mut unfollow_result = false;
        let mut found_log_entry = false;
        let mut following: HashSet<String> = HashSet::new();
        for res_log in res_deserialized_log.iter_mut() {
            if res_log.log.is_follow {
                if following.contains(&res_log.log.whom) {
                    res_log.valid = false
                } else {
                    following.insert(res_log.log.whom.clone());
                }
            } else {
                if following.contains(&res_log.log.whom) {
                    following.remove(&res_log.log.whom.to_string());
                } else {
                    res_log.valid = false;
                }
            }
        }
        for res_log in res_deserialized_log.iter() {
            if res_log.log.whom == whom && res_log.log.id == logical_clk {
                unfollow_result = res_log.valid;
                found_log_entry = true;
            }
        }
        if !(found_log_entry && unfollow_result) {
            return Err(Box::new(TribblerError::NotFollowing(
                who.to_string(),
                whom.to_string(),
            )));
        }
        // remove the invalid logs in the backend
        // let to_be_removed_logs: Vec<String> = res_deserialized_log
        //     .iter()
        //     .filter(|x| x.valid == false)
        //     .cloned()
        //     .collect::<Vec<LogResult>>()
        //     .iter()
        //     .map(|x| serde_json::to_string(&x.log).unwrap())
        //     .collect::<Vec<String>>();

        // for log in to_be_removed_logs.iter() {
        //     let _ = match user_bin
        //         .list_remove(&storage::KeyValue {
        //             key: KEY_FOLLOWING_LOG.to_string(),
        //             value: log.to_string(),
        //         })
        //         .await
        //     {
        //         Ok(_v) => {}
        //         Err(_e) => {} // we are not concerned if this errors; just cleanup failed
        //     };
        // }
        // no possible errors, return result to client
        Ok(())
    }

    /// Returns true when who following whom.
    /// Returns error when who == whom.
    /// Returns error when who or whom has not signed up.
    async fn is_following(&self, who: &str, whom: &str) -> TribResult<bool> {
        // return error if who == whom
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        if !is_valid_username(who) {
            return Err(Box::new(TribblerError::InvalidUsername(who.to_string())));
        }
        if !is_valid_username(whom) {
            return Err(Box::new(TribblerError::InvalidUsername(whom.to_string())));
        }

        // check if who and whom have signed up
        let special_sign_up_bin = match self.bin_storage.bin(BIN_SIGN_UP).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(registered_users_list) =
            match special_sign_up_bin.list_get(KEY_SIGNED_UP_USERS).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
        if !registered_users_list.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        if !registered_users_list.contains(&whom.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string())));
        }
        //fetch log
        let user_bin = match self.bin_storage.bin(who).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(fetched_log) = match user_bin.list_get(KEY_FOLLOWING_LOG).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let deserialized_log: Vec<FollowUnfollowLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<FollowUnfollowLog>>();
        let mut res_deserialized_log: Vec<LogResult> = deserialized_log
            .iter()
            .map(|x| LogResult {
                valid: true,
                log: x.clone(),
            })
            .collect::<Vec<LogResult>>();

        // parse log and identify the result of is_following for the log that is there at this moment
        let mut following: HashSet<String> = HashSet::new();
        for res_log in res_deserialized_log.iter_mut() {
            if res_log.log.is_follow {
                if following.contains(&res_log.log.whom) {
                    res_log.valid = false
                } else {
                    following.insert(res_log.log.whom.clone());
                }
            } else {
                if following.contains(&res_log.log.whom) {
                    following.remove(&res_log.log.whom.to_string());
                } else {
                    res_log.valid = false;
                }
            }
        }
        let is_following_result = following.contains(whom);
        // remove the invalid logs in the backend
        // let to_be_removed_logs: Vec<String> = res_deserialized_log
        //     .iter()
        //     .filter(|x| x.valid == false)
        //     .cloned()
        //     .collect::<Vec<LogResult>>()
        //     .iter()
        //     .map(|x| serde_json::to_string(&x.log).unwrap())
        //     .collect::<Vec<String>>();

        // for log in to_be_removed_logs.iter() {
        //     let _ = match user_bin
        //         .list_remove(&storage::KeyValue {
        //             key: KEY_FOLLOWING_LOG.to_string(),
        //             value: log.to_string(),
        //         })
        //         .await
        //     {
        //         Ok(_v) => {}
        //         Err(_e) => {} // we are not concerned if this errors; just cleanup failed
        //     };
        // }
        Ok(is_following_result)
    }

    /// Returns the list of following users.
    /// Returns error when who has not signed up.
    /// The list have users more than trib.MaxFollowing=2000,
    /// if and only if the 2000'th user is generate d by concurrent Follow()
    /// calls.
    async fn following(&self, who: &str) -> TribResult<Vec<String>> {
        // check if who have signed up
        if !is_valid_username(who) {
            return Err(Box::new(TribblerError::InvalidUsername(who.to_string())));
        }

        let special_sign_up_bin = match self.bin_storage.bin(BIN_SIGN_UP).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(registered_users_list) =
            match special_sign_up_bin.list_get(KEY_SIGNED_UP_USERS).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
        if !registered_users_list.contains(&who.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string())));
        }
        //fetch log
        let user_bin = match self.bin_storage.bin(who).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(fetched_log) = match user_bin.list_get(KEY_FOLLOWING_LOG).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let deserialized_log: Vec<FollowUnfollowLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<FollowUnfollowLog>>();
        let mut res_deserialized_log: Vec<LogResult> = deserialized_log
            .iter()
            .map(|x| LogResult {
                valid: true,
                log: x.clone(),
            })
            .collect::<Vec<LogResult>>();

        // parse log and identify the result of following() for the log that is there at this moment
        let mut following: HashSet<String> = HashSet::new();
        for res_log in res_deserialized_log.iter_mut() {
            if res_log.log.is_follow {
                if following.contains(&res_log.log.whom) {
                    res_log.valid = false
                } else {
                    following.insert(res_log.log.whom.clone());
                }
            } else {
                if following.contains(&res_log.log.whom) {
                    following.remove(&res_log.log.whom.to_string());
                } else {
                    res_log.valid = false;
                }
            }
        }
        // remove the invalid logs in the backend
        // let to_be_removed_logs: Vec<String> = res_deserialized_log
        //     .iter()
        //     .filter(|x| x.valid == false)
        //     .cloned()
        //     .collect::<Vec<LogResult>>()
        //     .iter()
        //     .map(|x| serde_json::to_string(&x.log).unwrap())
        //     .collect::<Vec<String>>();

        // for log in to_be_removed_logs.iter() {
        //     let _ = match user_bin
        //         .list_remove(&storage::KeyValue {
        //             key: KEY_FOLLOWING_LOG.to_string(),
        //             value: log.to_string(),
        //         })
        //         .await
        //     {
        //         Ok(_v) => {}
        //         Err(_e) => {} // we are not concerned if this errors; just cleanup failed
        //     };
        // }
        let vec_list: Vec<String> = following.into_iter().collect();
        Ok(vec_list)
    }

    /// List the tribs of someone's following users (including himself).
    /// Returns error when user has not signed up.
    async fn home(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        }
        let special_sign_up_bin = match self.bin_storage.bin(BIN_SIGN_UP).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(registered_users_list) =
            match special_sign_up_bin.list_get(KEY_SIGNED_UP_USERS).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
        if !registered_users_list.contains(&user.to_string()) {
            return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string())));
        }
        // generate timeline
        let mut home_timeline: Vec<SeqTrib> = vec![];
        // get user's tribs
        let user_bin = match self.bin_storage.bin(user).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let storage::List(trib_list) = match user_bin.list_get(KEY_TRIBS).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let deserialized_trib_list: Vec<Trib> = trib_list
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<Trib>>();
        let mut user_trib_list: Vec<SeqTrib> = deserialized_trib_list
            .iter()
            .map(|x| SeqTrib {
                logical_clock: x.clock,
                physical_clock: x.time,
                user_name: x.user.to_string(),
                message: x.message.to_string(),
                trib: Arc::<Trib>::new(x.clone()),
            })
            .collect::<Vec<SeqTrib>>();
        user_trib_list.sort();
        let ntrib = user_trib_list.len();
        let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => ntrib - MAX_TRIB_FETCH,
            _ => 0,
        };
        // clean user's tribs
        if ntrib > MAX_TRIB_FETCH {
            let to_be_removed_tribs: Vec<String> = user_trib_list[..start]
                .to_vec()
                .iter()
                .map(|x| serde_json::to_string(&(x.trib.clone())).unwrap())
                .collect::<Vec<String>>();

            for trib in to_be_removed_tribs.iter() {
                let _ = match user_bin
                    .list_remove(&storage::KeyValue {
                        key: KEY_TRIBS.to_string(),
                        value: trib.to_string(),
                    })
                    .await
                {
                    Ok(_v) => {}
                    Err(_e) => {} // we are not concerned if this errors; just cleanup failed
                };
            }
        }
        // append user's tribs to timeline
        home_timeline.append(&mut user_trib_list[start..].to_vec().clone());

        // identify followers
        let storage::List(fetched_log) = match user_bin.list_get(KEY_FOLLOWING_LOG).await {
            Ok(v) => v,
            Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
        };
        let deserialized_log: Vec<FollowUnfollowLog> = fetched_log
            .iter()
            .map(|x| serde_json::from_str(&x).unwrap())
            .collect::<Vec<FollowUnfollowLog>>();
        let mut res_deserialized_log: Vec<LogResult> = deserialized_log
            .iter()
            .map(|x| LogResult {
                valid: true,
                log: x.clone(),
            })
            .collect::<Vec<LogResult>>();

        // parse log and identify the result of following() for the log that is there at this moment
        let mut following: HashSet<String> = HashSet::new();
        for res_log in res_deserialized_log.iter_mut() {
            if res_log.log.is_follow {
                if following.contains(&res_log.log.whom) {
                    res_log.valid = false
                } else {
                    following.insert(res_log.log.whom.clone());
                }
            } else {
                if following.contains(&res_log.log.whom) {
                    following.remove(&res_log.log.whom.to_string());
                } else {
                    res_log.valid = false;
                }
            }
        }
        // remove the invalid logs in the backend
        let to_be_removed_logs: Vec<String> = res_deserialized_log
            .iter()
            .filter(|x| x.valid == false)
            .cloned()
            .collect::<Vec<LogResult>>()
            .iter()
            .map(|x| serde_json::to_string(&x.log).unwrap())
            .collect::<Vec<String>>();

        for log in to_be_removed_logs.iter() {
            let _ = match user_bin
                .list_remove(&storage::KeyValue {
                    key: KEY_FOLLOWING_LOG.to_string(),
                    value: log.to_string(),
                })
                .await
            {
                Ok(_v) => {}
                Err(_e) => {} // we are not concerned if this errors; just cleanup failed
            };
        }
        let following_list: Vec<String> = following.into_iter().collect();
        // fetch tribs of following
        for following_user in following_list.iter() {
            let following_user_bin = match self.bin_storage.bin(following_user).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
            let storage::List(trib_list) = match following_user_bin.list_get(KEY_TRIBS).await {
                Ok(v) => v,
                Err(e) => return Err(Box::new(TribblerError::Unknown(e.to_string()))), // some error from remote service; sign_up not successful
            };
            let deserialized_trib_list: Vec<Trib> = trib_list
                .iter()
                .map(|x| serde_json::from_str(&x).unwrap())
                .collect::<Vec<Trib>>();
            let mut user_trib_list: Vec<SeqTrib> = deserialized_trib_list
                .iter()
                .map(|x| SeqTrib {
                    logical_clock: x.clock,
                    physical_clock: x.time,
                    user_name: x.user.to_string(),
                    message: x.message.to_string(),
                    trib: Arc::<Trib>::new(x.clone()),
                })
                .collect::<Vec<SeqTrib>>();
            user_trib_list.sort();
            let ntrib = user_trib_list.len();
            let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
                Ordering::Greater => ntrib - MAX_TRIB_FETCH,
                _ => 0,
            };
            // clean user's tribs
            if ntrib > MAX_TRIB_FETCH {
                let to_be_removed_tribs: Vec<String> = user_trib_list[..start]
                    .to_vec()
                    .iter()
                    .map(|x| serde_json::to_string(&(x.trib.clone())).unwrap())
                    .collect::<Vec<String>>();

                for trib in to_be_removed_tribs.iter() {
                    let _ = match following_user_bin
                        .list_remove(&storage::KeyValue {
                            key: KEY_TRIBS.to_string(),
                            value: trib.to_string(),
                        })
                        .await
                    {
                        Ok(_v) => {}
                        Err(_e) => {} // we are not concerned if this errors; just cleanup failed
                    };
                }
            }
            // append user's tribs to timeline
            home_timeline.append(&mut user_trib_list[start..].to_vec().clone());
        }
        // sort the timeline
        home_timeline.sort();
        let n = home_timeline.len();
        let s = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => n - MAX_TRIB_FETCH,
            _ => 0,
        };
        let res_list: Vec<Arc<Trib>> = home_timeline[s..]
            .to_vec()
            .iter()
            .map(|x| x.trib.clone())
            .collect::<Vec<Arc<Trib>>>();
        Ok(res_list)
    }
}
