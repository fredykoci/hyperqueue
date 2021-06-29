use crate::{JobId, JobTaskId};
use serde::Deserialize;
use serde::Serialize;
use std::num::NonZeroU64;

pub type ChannelId = u32;

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterStream {
    pub job: JobId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StartTaskStreamMsg {
    pub task: JobTaskId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataMsg {
    pub task: JobTaskId,
    pub channel: ChannelId,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EndTaskStreamMsg {
    pub task: JobTaskId,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FromStreamerMessage {
    Start(StartTaskStreamMsg),
    Data(DataMsg),
    End(EndTaskStreamMsg),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EndTaskStreamResponseMsg {
    pub task: JobTaskId,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ToStreamerMessage {
    Error(String),
    EndResponse(EndTaskStreamResponseMsg),
}
