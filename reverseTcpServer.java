import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
// 并发
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class reverseTcpServer {
    public static final byte _initialization = 0x01;                                            // 一个字节8位，对应十六进制的2位
    public static final byte _agreement = 0x02;
    public static final byte _clientToServer = 0x03;
    public static final byte _serverToClient = 0x04;
    public static final int _headerSize = 6;                                                    // 头部字段长度
    // ——————————————————————————————————————————————————————————
    private Selector selector;                                                                  // 用于管理多个通道的选择器
    private final Map<SocketChannel, byte[]> containers = new ConcurrentHashMap<>();
    private final Map<SocketChannel, Integer> numberOfSegments = new ConcurrentHashMap<>();
    private final Map<SocketChannel, Object> channelLocks = new ConcurrentHashMap<>();

    private final ExecutorService executorService;                                              // 线程池
    private final int _workers = 10;                                                            // 工人数


    // 构造方法
    public reverseTcpServer(){
        executorService = Executors.newFixedThreadPool(_workers);
    }

    // 主方法
    public static void main(String[] args) throws IOException {
        reverseTcpServer server = new reverseTcpServer();
        server.startServer(12345);  // 启动服务器，监听端口12345
    }

    // 启动服务器
    public void startServer(int port) throws IOException {
        selector = Selector.open();                                     // 创建一个Selector对象
        ServerSocketChannel serverSocket = ServerSocketChannel.open();  // 打开一个serverSocketChannel， 监听新进来的TCP连接，对每一个连接都创建一个SocketChannel（通过TCP读写网络中的数据）
        serverSocket.bind(new InetSocketAddress(port));                 // 绑定一个服务器端口
        serverSocket.configureBlocking(false);                          // 设置为非阻塞模式
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);        // 注册感兴趣的I/O事件：接受连接

        System.out.println("Server is listening on port " + port);
        // 准备工作完成
        while (true) {
            int readyChannels = selector.select();  // 阻塞，直到至少有一个通道准备好
            if(readyChannels == 0) continue;        // 增强健壮性
            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();   // SelectionKey对象的集合，代表了准备好进行I/O操作的通道

            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();                  // 从集合中移除已经处理的键 解绑

                if (!key.isValid()) continue;
                if (key.isAcceptable()) {                                           // 处理连接事件
                    accept(key);
                } else if (key.isReadable()) {
                    read(key);
                    // executorService.submit(() -> handleRead(key));                 // 处理读事件
                } else if(key.isWritable()){
                    write(key);
                    // executorService.submit(() -> handleWrite(key));                // 处理写事件
                }
            }
        }
    }

    // 处理连接请求
    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverSocket = (ServerSocketChannel) key.channel();
        SocketChannel client = serverSocket.accept();   // 接受客户端链接
        client.configureBlocking(false);                // 配置客户端通道为非阻塞模式
        client.register(selector, SelectionKey.OP_READ);// 将客户端通道注册到选择器，监听读事件

//        // 获取客户端的远程地址
//        String clientAddress = client.getRemoteAddress().toString();
//        // 将IP地址中的斜杠替换成下划线，以避免文件夹名称中出现不允许的字符
//        String clientFolderName = clientAddress.replace("/", "_");
//        // 构建客户端文件夹的路径
//        Path clientFolder = Paths.get("/Users/lloyd/temp", clientFolderName);
//        // 如果文件夹不存在，则创建该文件夹
//        if (!Files.exists(clientFolder)) {
//            Files.createDirectories(clientFolder);
//        }
//        tempFolder.put(client, clientFolder);

        channelLocks.put(client, new Object());         // 初始化锁对象
        System.out.println("Accepted connection from " + client);
    }

    // 处理反转文本请求
    private void read(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();   // 获取客户端通道
        ByteBuffer buffer = ByteBuffer.allocate(1024);  // 独立缓冲区
        ByteBuffer headerBuffer = ByteBuffer.allocate(_headerSize);

        int bytesRead = 0;
        // 读取头部信息
        while(headerBuffer.hasRemaining()) {
            bytesRead = client.read(headerBuffer);
            if (bytesRead == -1) {
                client.close();                       // 客户端关了
                return;
            }
        }
        headerBuffer.flip();                          // limit -> position position -> 0
        short messageType = headerBuffer.getShort();  // 前两个字节 type

        if(messageType == _initialization){
            int N = headerBuffer.getInt();            // 后四个字节    就是N
            headerBuffer.clear();

            System.out.println("successfully received initialization( " + client.getRemoteAddress() + " ) N: " + N);
            numberOfSegments.put(client, N);
            client.register(selector, SelectionKey.OP_READ);    // 注册写

            // 发送agree报文
            headerBuffer.putShort(_agreement);
            headerBuffer.flip();

            while(headerBuffer.hasRemaining()){
                client.write(headerBuffer);
            }
            headerBuffer.clear();
        } else if (messageType == _clientToServer){
            int segmentSize = headerBuffer.getInt();
            headerBuffer.clear();

            byte[] messageContent = new byte[segmentSize];
            containers.put(client, messageContent);
            System.out.println("Received from client: " + client.getRemoteAddress() + " | segmentSize: " + segmentSize + " Byte.");

            bytesRead = 0;                                  // bytesRead是每轮开始前已经读到的长度
            while(bytesRead < messageContent.length){
                // 调整buffer的limit 不能从channel里面拿出本轮不该拿的数据
                int leftBytesToRead = Math.min(buffer.remaining(), messageContent.length - bytesRead);  // 想要读到的长度
                buffer.limit(leftBytesToRead);
                int bytesReadLength = client.read(buffer);    // 实际读到的长度
                if(bytesReadLength == 0) continue;
                buffer.flip();
                buffer.get(messageContent, bytesRead, bytesReadLength);
                bytesRead += bytesReadLength;
                buffer.clear();
            }
            String receivedString = new String(messageContent);
            System.out.println("Received String: " + receivedString);
            String reversedString = new StringBuilder(receivedString).reverse().toString(); // 反转字符串
            byte[] reversedData = reversedString.getBytes();                                // 将反转后的字符串转换为字节数组
            containers.put(client, reversedData);

            int n = numberOfSegments.get(client);
            numberOfSegments.put(client, n - 1);

            client.register(selector, SelectionKey.OP_WRITE);       // 注册写
        } else{
            System.out.println("意料之外");
        }

    }

    private void write(SelectionKey key) throws IOException{
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);  // 独立缓冲区
        byte[] messageContent = containers.get(client);

        sendMessage(buffer, client, _serverToClient, messageContent);
        int n = numberOfSegments.get(client);

        if(n != 0){
            client.register(selector, SelectionKey.OP_READ);
        } else{
            containers.remove(client);
            numberOfSegments.remove(client);
            int bytesRead;
            // 等客户端关闭，我的服务器也要关，——————————————优化
            while((bytesRead = client.read(buffer)) != -1){buffer.clear();}
            client.close();
            System.out.println("Client closed");
        }
    }

    private void handleRead(SelectionKey key){
        SocketChannel client = (SocketChannel) key.channel();
        Object lock = channelLocks.get(client);
        synchronized (lock) {
            try {
                read(key);
            } catch (IOException e) {
                key.cancel();
                try {
                    key.channel().close();
                } catch (IOException ex) {
                    System.err.println("Error closing client channel: " + ex.getMessage());
                }
            }
        }
    }

    private void handleWrite(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        Object lock = channelLocks.get(client);
        synchronized (lock) {
            try {
                write(key);
            } catch (IOException e) {
                System.err.println("Error writing to client: " + e.getMessage());
                key.cancel();
                try {
                    key.channel().close();
                } catch (IOException ex) {
                    System.err.println("Error closing client channel: " + ex.getMessage());
                }
            }
        }

    }

    private void sendMessage(ByteBuffer buffer, SocketChannel client, byte messageType, byte[] messageContent) throws IOException{
        // 只负责发送普通报文
        buffer.putShort(messageType);           // short是2字节
        buffer.putInt(messageContent.length);   // int是四字节
        buffer.flip();
        while(buffer.hasRemaining()){           // 检查的是position和limit之间的距离
            client.write(buffer);
        }
        buffer.clear();
        int bytesWrittern = 0;
        while(bytesWrittern < messageContent.length){
            int remaining = messageContent.length - bytesWrittern;
            int writeSize = Math.min(remaining, buffer.remaining());    // buffer的剩余写入量，容器中还有多少数据没写进去
            buffer.put(messageContent, bytesWrittern, writeSize);       // 从messageContent的offset开始写，写进去length个
            bytesWrittern += writeSize;
            buffer.flip();
            while(buffer.hasRemaining()){           // 检查的是position和limit之间的距离
                client.write(buffer);               // 防止操作系统内核中的套接字发送缓冲区满了
            }
            buffer.clear();
        }
    }
}
