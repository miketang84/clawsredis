use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use test001::{handle_client, KVStore};

fn spawn_one_connection_server() -> (std::net::SocketAddr, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
    let addr = listener.local_addr().expect("listener should expose local addr");
    let store = Arc::new(Mutex::new(KVStore::new()));

    let handle = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("test client should connect");
        handle_client(stream, store).expect("server should handle the test client");
    });

    (addr, handle)
}

fn send_and_expect(stream: &mut TcpStream, request: &[u8], expected: &[u8]) {
    stream.write_all(request).expect("request should be written");
    let mut response = vec![0_u8; expected.len()];
    stream
        .read_exact(&mut response)
        .expect("expected response bytes should be read");
    assert_eq!(response, expected);
}

#[test]
fn server_handles_set_get_keys_del_and_nil_miss() {
    let (addr, server) = spawn_one_connection_server();
    let mut client = TcpStream::connect(addr).expect("client should connect to server");
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("read timeout should be set");
    client
        .set_write_timeout(Some(Duration::from_secs(1)))
        .expect("write timeout should be set");

    send_and_expect(
        &mut client,
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        b"+OK\r\n",
    );
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
        b"$3\r\nbar\r\n",
    );
    send_and_expect(&mut client, b"*1\r\n$4\r\nKEYS\r\n", b"*1\r\n$3\r\nfoo\r\n");
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n",
        b":1\r\n",
    );
    send_and_expect(
        &mut client,
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
        b"$-1\r\n",
    );
    send_and_expect(&mut client, b"*1\r\n$4\r\nQUIT\r\n", b"+Goodbye!\r\n");

    server.join().expect("server thread should exit after QUIT");
}
