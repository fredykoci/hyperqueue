use crate::client::job::job_status;
use crate::common::WrappedRcRefCell;
use crate::transfer::stream::{FromStreamerMessage, ToStreamerMessage};
use crate::{JobId, Map};
use std::sync::mpsc::Sender;
use std::time::Duration;
use tako::TaskId;
use tokio::sync::oneshot;

const STREAMER_BUFFER_SIZE: usize = 128;

struct StreamInfo {
    sender: Sender<FromStreamerMessage>,
    response: Map<TaskId, oneshot::Sender<Option<String>>>,
}

pub struct Streamer {
    streams: Map<JobId, StreamInfo>,
}

impl Streamer {
    pub fn get_stream(&mut self, job_id: JobId) -> Sender<FromStreamerMessage> {
        if let Some(ref mut info) = self.streams.get_mut(&job_id) {
            info.n_tasks += 1;
            info.sender.clone()
        } else {
            todo!()
        }
    }
}

pub type StreamerRef = WrappedRcRefCell<Streamer>;

impl StreamerRef {
    pub fn start(stream_clean_interval: Duration) -> StreamerRef {
        let streamer_ref = WrappedRcRefCell::wrap(Streamer {
            streams: Default::default(),
        });

        let streamer_ref2 = streamer_ref.clone();
        tokio::task::spawn_local(async move {
            let mut it = tokio::time::interval(stream_clean_interval);
            loop {
                it.tick().await;
                let mut streamer = streamer_ref2.get_mut();
                streamer.streams.retain(|_, info| info.n_tasks == 0);
            }
        });

        streamer_ref
    }
}
