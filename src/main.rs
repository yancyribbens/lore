use bitcoinkernel::{Context, ChainType, KernelError};
use bitcoinkernel::ChainstateManagerBuilder;
use bitcoinkernel::ChainstateManager;

use std::sync::Arc;
use std::sync::mpsc;
use std::sync::Mutex;
use std::thread::available_parallelism;
use std::net::SocketAddr;

use bitcoin::network::Network;

use bitcoin_primitives::BlockHash;
use bitcoin_p2p_messages::Address;
use bitcoin_p2p_messages::ServiceFlags;
use bitcoin_p2p_messages::ProtocolVersion;
use bitcoin_p2p_messages::Magic;
use bitcoin_p2p_messages::NetworkExt;
use bitcoin_p2p_messages::message::{NetworkMessage, RawNetworkMessage};
use bitcoin_p2p_messages::message::AddrV2Payload;
use bitcoin_p2p_messages::message_network::VersionMessage;
use bitcoin_p2p_messages::message_network::ClientSoftwareVersion;
use bitcoin_p2p_messages::message_network::UserAgent;
use bitcoin_p2p_messages::message_network::UserAgentVersion;
use bitcoin_p2p_messages::message_blockdata::GetBlocksMessage;

use std::time::{SystemTime, UNIX_EPOCH};
use std::net::TcpStream;

use std::net::IpAddr;
use std::net::Ipv4Addr;

const LOCAL:(u8, u8, u8, u8) = (127, 0, 0, 1);
const LOCAL_PORT: u16 = 54321;

const DATA_DIR: &str = "/tmp";
const BLOCKS_DIR: &str = "/tmp/blocks";

const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::INVALID_CB_NO_BAN_VERSION;

#[derive(Clone)]
pub struct TipState {
    pub block_hash: bitcoin_primitives::BlockHash,
}

impl Default for TipState {
    fn default() -> Self {
        Self {
            block_hash: BlockHash::GENESIS_PREVIOUS_BLOCK_HASH,
        }
    }
}

fn chain_context() -> Result<Context, KernelError>  {
    // TODO parse different chains.
    let chain_type = ChainType::Testnet4;
    let context = Context::builder().chain_type(chain_type).build()?;

    Ok(context)
}

impl NodeState {
    pub fn set_tip_state(&self, block_hash: BlockHash) {
        let mut state = self.tip_state.lock().unwrap();
        state.block_hash = block_hash;
    }

    pub fn get_tip_state(&self) -> TipState {
        let state = self.tip_state.lock().unwrap();
        state.clone()
    }
}

pub struct NodeState {
    pub addr_tx: mpsc::Sender<AddrV2Payload>,
    pub block_tx: mpsc::SyncSender<bitcoinkernel::Block>,
    pub tip_state: Arc<Mutex<TipState>>,
    pub context: Arc<Context>,
    pub chainman: Arc<ChainstateManager>,
}

pub fn create_version_message() -> VersionMessage {
    let (la, lb, lc, ld) = LOCAL;

    let local_socket: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(la, lb, lc, ld)), LOCAL_PORT);

    let service_flags = ServiceFlags::NONE;

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let receiver = Address::new(&local_socket, ServiceFlags::NONE);
    let sender = Address::new(&local_socket, ServiceFlags::NONE);

    let client_software_version = ClientSoftwareVersion::SemVer {
        major: 0,
        minor: 0,
        revision: 1
    };

    let user_agent = UserAgent::new(
        "lore",
        &UserAgentVersion::new(client_software_version)
    );

    VersionMessage::new(
        // The P2P network protocol version
        PROTOCOL_VERSION,
        // A bitmask describing the services supported by this node
        service_flags,
        // The time at which the `version` message was sent
        timestamp,
        // The network address of the peer receiving the message
        receiver,
        // The network address of the peer sending the message
        sender,
        // A random nonce used to detect loops in the network
        //
        // The nonce can be used to detect situations when a node accidentally
        // connects to itself. Set it to a random value and, in case of incoming
        // connections, compare the value - same values mean self-connection.
        //
        // If your application uses P2P to only fetch the data and doesn't listen
        // you may just set it to 0.
        0,
        // A string describing the peer's software
        user_agent,
        // The height of the maximum-work blockchain that the peer is aware of
        0,
    )
}

use bitcoin_p2p_messages::message;
use bitcoin_p2p_messages::message::V1MessageHeader;
use bitcoin_p2p_messages::message_network;

pub fn network_start(_chainman: ChainstateManager, network: Network, remote: Ipv4Addr) {
    let magic: Magic = Magic::try_from(network).unwrap();
    let port = network.default_p2p_port();

    let version_message = create_version_message();
    let init_msg = NetworkMessage::Version(version_message);
    let raw_msg = RawNetworkMessage::new(magic, init_msg);

    let remote_socket: SocketAddr = SocketAddr::new(IpAddr::V4(remote), port);
    if let Ok(mut stream) = TcpStream::connect(remote_socket) {
        // Send the message
        encoding::encode_to_writer(&raw_msg, &mut stream).unwrap();
        println!("Sent version message");

        // Setup StreamReader
        let read_stream = stream.try_clone().unwrap();
        let mut stream_reader = std::io::BufReader::new(read_stream);
        loop {
            //let reply = NetworkMessage::decode(&mut stream_reader).unwrap();
            let V1MessageHeader {command, ..} = encoding::decode_from_read::<V1MessageHeader, _>(&mut stream_reader).unwrap();

            // consume the remainnder of the buffer returning the message
            match command.as_ref() {
                "alert" => {
                    let msg =
                        encoding::decode_from_read::<message_network::Alert, _>(&mut stream_reader)
                            .unwrap();
                    println!("msg {:?}", msg);
                },
                "ping" => {
                    println!("got ping");
                    //NetworkMessage::Ping(nonce) => {
                        //println!("got a ping with nonce: {}", nonce);

                        //let pong_msg = NetworkMessage::Pong(*nonce);
                        //let raw_msg = RawNetworkMessage::new(
                            //magic,
                            //pong_msg
                        //);
                        //encoding::encode_to_writer(&raw_msg, &mut stream).unwrap();
                },
                "sendcmpct" => println!("sendcmpct"),
                "verack" => {
                    println!("received verack");
                },
                "version" => {
                    let msg =
                        encoding::decode_from_read::<VersionMessage, _>(&mut stream_reader)
                            .unwrap();
                    println!("msg {:?}", msg);

                    let second_message = RawNetworkMessage::new(
                        magic,
                        NetworkMessage::Verack,
                    );

                    encoding::encode_to_writer(&second_message, &mut stream).unwrap();
                    println!("sent verack back in response to version");
                },
                _ => unimplemented!("{:?}", command.as_ref())
            }

            //match reply {
                //NetworkMessage::Version(v) => {
                    //println!("payload {:?}", reply);
                    //println!("Received version message: {:?}", v);

                    //let second_message = RawNetworkMessage::new(
                        //magic,
                        //NetworkMessage::Verack,
                    //);

                    //encoding::encode_to_writer(&second_message, &mut stream).unwrap();
                    //println!("Sent verack message");
                //}
                //NetworkMessage::Verack => {
                    //println!("Received verack message: {:?}", reply);

                    //let get_blocks_msg = NetworkMessage::GetBlocks(GetBlocksMessage {
                        //version: PROTOCOL_VERSION,
                        //locator_hashes: vec![],
                        //stop_hash: BlockHash::from_byte_array([0xab; 32]),
                    //});

                    //let raw_msg = RawNetworkMessage::new(
                        //magic,
                        //get_blocks_msg
                    //);

                    //encoding::encode_to_writer(&raw_msg, &mut stream).unwrap();
                //}
                //NetworkMessage::Ping(nonce) => {
                    //println!("got a ping with nonce: {}", nonce);

                    //let pong_msg = NetworkMessage::Pong(*nonce);
                    //let raw_msg = RawNetworkMessage::new(
                        //magic,
                        //pong_msg
                    //);
                    //encoding::encode_to_writer(&raw_msg, &mut stream).unwrap();
                //}
                //NetworkMessage::Addr(_) => println!("addr"),
                //NetworkMessage::Inv(_) => println!("inv"),
                //NetworkMessage::GetData(_) => println!("getdata"),
                //NetworkMessage::NotFound(_) => println!("notfound"),
                //NetworkMessage::GetBlocks(_) => println!("getblocks"),
                //NetworkMessage::GetHeaders(_) => println!("getheaders"),
                //NetworkMessage::MemPool => println!("mempool"),
                //NetworkMessage::Tx(_) => println!("tx"),
                //NetworkMessage::Block(_) => println!("block"),
                //NetworkMessage::Headers(_) => println!("headers"),
                //NetworkMessage::SendHeaders => println!("sendheaders"),
                //NetworkMessage::GetAddr => println!("getaddr"),
                //NetworkMessage::Pong(_) => println!("pong"),
                //NetworkMessage::MerkleBlock(_) => println!("merkleblock"),
                //NetworkMessage::FilterLoad(_) => println!("filterload"),
                //NetworkMessage::FilterAdd(_) => println!("filteradd"),
                //NetworkMessage::FilterClear => println!("filterclear"),
                //NetworkMessage::GetCFilters(_) => println!("getcfilters"),
                //NetworkMessage::CFilter(_) => println!("cfilter"),
                //NetworkMessage::GetCFHeaders(_) => println!("getcfheaders"),
                //NetworkMessage::CFHeaders(_) => println!("cfheaders"),
                //NetworkMessage::GetCFCheckpt(_) => println!("getcfcheckpt"),
                //NetworkMessage::CFCheckpt(_) => println!("cfcheckpt"),
                //NetworkMessage::SendCmpct(_) => println!("sendcmpct"),
                //NetworkMessage::CmpctBlock(_) => println!("cmpctblock"),
                //NetworkMessage::GetBlockTxn(_) => println!("getblocktxn"),
                //NetworkMessage::BlockTxn(_) => println!("blocktxn"),
                //NetworkMessage::Alert(a) => {
                    //println!("got alert: {:?}", a)
                //},
                //NetworkMessage::Reject(_) => println!("reject"),
                //NetworkMessage::FeeFilter(_) => println!("feefilter"),
                //NetworkMessage::WtxidRelay => println!("wtxidrelay"),
                //NetworkMessage::AddrV2(_) => println!("addrv2"),
                //NetworkMessage::SendAddrV2 => println!("sendaddrv2"),
                //NetworkMessage::Unknown { .. } => println!("unknown"),
            //}
        }
        let _ = stream.shutdown(std::net::Shutdown::Both);
    } else {
        eprintln!("failed to open connection");
    }
}

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    network: Network,

    #[arg(long, short, default_value = "127.0.0.1")]
    remote_address: Ipv4Addr,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = chain_context()?;
    let arc_context = Arc::new(context);
    let chainman_builder = ChainstateManagerBuilder::new(&arc_context, &DATA_DIR, &BLOCKS_DIR)?
        .worker_threads(((available_parallelism().unwrap().get() / 2) + 1).try_into()?,
    );
    let chainman = chainman_builder.build()?;
    chainman.import_blocks()?;
    println!("Bitcoin kernel initialized");

    let args = Args::parse();
    let network: Network = args.network;
    let remote: Ipv4Addr = args.remote_address;

    network_start(chainman, network, remote);

    Ok(())
}
