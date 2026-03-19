use std::net::SocketAddr;

use bitcoin::network::Network;

use bitcoin_p2p_messages::Address;
use bitcoin_p2p_messages::ServiceFlags;
use bitcoin_p2p_messages::ProtocolVersion;
use bitcoin_p2p_messages::Magic;
use bitcoin_p2p_messages::NetworkExt;
use bitcoin_p2p_messages::message::{NetworkMessage, V1NetworkMessage};
use bitcoin_p2p_messages::message_network::VersionMessage;
use bitcoin_p2p_messages::message_network::ClientSoftwareVersion;
use bitcoin_p2p_messages::message_network::UserAgent;
use bitcoin_p2p_messages::message_network::UserAgentVersion;

use bitcoin_p2p_messages::message;
use bitcoin_p2p_messages::message::V1MessageHeader;
use bitcoin_p2p_messages::message_network;

use std::time::{SystemTime, UNIX_EPOCH};
use std::net::TcpStream;

use std::net::IpAddr;
use std::net::Ipv4Addr;

use clap::Parser;

const LOCAL:(u8, u8, u8, u8) = (127, 0, 0, 1);
const LOCAL_PORT: u16 = 54321;
const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::INVALID_CB_NO_BAN_VERSION;

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

pub fn network_start(network: Network, remote: Ipv4Addr) {
    let magic: Magic = Magic::try_from(network).unwrap();
    let port = network.default_p2p_port();

    let version_message = create_version_message();
    let init_msg = NetworkMessage::Version(version_message);
    let raw_msg = V1NetworkMessage::new(magic, init_msg);

    let remote_socket: SocketAddr = SocketAddr::new(IpAddr::V4(remote), port);
    if let Ok(mut stream) = TcpStream::connect(remote_socket) {
        encoding::encode_to_writer(&raw_msg, &mut stream).unwrap();
        println!("Sent version message");

        let read_stream = stream.try_clone().unwrap();
        let mut stream_reader = std::io::BufReader::new(read_stream);
        loop {
            let V1MessageHeader {command, ..} = encoding::decode_from_read::<V1MessageHeader, _>(&mut stream_reader).unwrap();

            match command.as_ref() {
                "alert" => {
                    let msg =
                        encoding::decode_from_read::<message_network::Alert, _>(&mut stream_reader)
                            .unwrap();
                    println!("msg {:?}", msg);
                },
                "ping" => {
                    println!("got ping");
                    let msg = encoding::decode_from_read::<bitcoin_p2p_messages::message::Ping, _>(
                        &mut stream_reader
                    ).unwrap();

                    let pong = bitcoin_p2p_messages::message::Pong::from_ping(&msg);

                    let msg_header = V1MessageHeader::new(magic, &pong, "pong");
                    println!("send pong header");
                    let _ = encoding::encode_to_writer(&msg_header, &stream);

                    println!("send pong");
                    let _ = encoding::encode_to_writer(&pong, &stream);
                },
                "sendcmpct" => {
                    let msg = encoding::decode_from_read::<bitcoin_p2p_messages::message_compact_blocks::SendCmpct, _>(&mut stream_reader).unwrap();
                    println!("decoded sendcmpct: {:?}", msg);

                },
                "verack" => {
                    println!("received verack");
                },
                "version" => {
                    let msg =
                        encoding::decode_from_read::<VersionMessage, _>(&mut stream_reader)
                            .unwrap();
                    println!("msg {:?}", msg);

                    let second_message = V1NetworkMessage::new(
                        magic,
                        NetworkMessage::Verack,
                    );

                    encoding::encode_to_writer(&second_message, &mut stream).unwrap();
                    println!("sent verack back in response to version");
                },
                "feefilter" => {
                    let msg = encoding::decode_from_read::<message::FeeFilter, _>(&mut stream_reader).unwrap();
                    println!("got feefilter {:?}", msg);
                },
                _ => unimplemented!("{:?}", command.as_ref())
            }
        }
    } else {
        eprintln!("failed to open connection");
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    network: Network,

    #[arg(long, short, default_value = "127.0.0.1")]
    remote_address: Ipv4Addr,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let network: Network = args.network;
    let remote: Ipv4Addr = args.remote_address;

    network_start(network, remote);

    Ok(())
}
