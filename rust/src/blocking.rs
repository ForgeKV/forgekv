/// Blocking operations support (BLPOP, BRPOP, BLMOVE, BLMPOP).
///
/// Uses a global broadcast channel: every LPUSH/RPUSH/LINSERT/etc.
/// broadcasts (db, key) so that waiting BLPOP clients can wake up and
/// attempt to pop.  The broadcast approach is simple, correct, and
/// requires no per-command structs to carry extra fields.
use lazy_static::lazy_static;
use tokio::sync::broadcast;

/// Notification payload: (db_index, key_bytes)
pub type PushNotif = (usize, Vec<u8>);

pub struct BlockingNotifier {
    tx: broadcast::Sender<PushNotif>,
}

impl BlockingNotifier {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(65536);
        BlockingNotifier { tx }
    }

    /// Called by LPUSH/RPUSH/LINSERT after a successful write.
    pub fn notify(&self, db: usize, key: Vec<u8>) {
        // Ignore send error (no active subscribers is fine)
        let _ = self.tx.send((db, key));
    }

    /// Subscribe to push notifications.
    pub fn subscribe(&self) -> broadcast::Receiver<PushNotif> {
        self.tx.subscribe()
    }
}

lazy_static! {
    pub static ref BLOCKING_NOTIFIER: BlockingNotifier = BlockingNotifier::new();
}
