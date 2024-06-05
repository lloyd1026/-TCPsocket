import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import java.util.Stack;
// 主类
public class reverseTcpClient {
    public static final byte _initialization = 0x01;                        // 一个字节8位，对应十六进制的2位
    public static final byte _agreement = 0x02;
    public static final byte _clientToServer = 0x03;
    public static final byte _serverToClient = 0x04;
    public static final int _headerSize = 6;                                // 头部字段长度
    private final ByteBuffer buffer = ByteBuffer.allocate(1024);    // position limit capacity
    // 主方法
    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("需要5个参数[ip, 端口, 分段最小长度, 分段最大长度, 源文件地址]");
            return;
        }

        String serverIp = args[0];
        int serverPort = Integer.parseInt(args[1]);
        // 随机分段的长度限定范围，最后一块除外（最后一块可能会小于Lmin）
        int Lmin = Integer.parseInt(args[2]);
        int Lmax = Integer.parseInt(args[3]);
        String filePath = args[4];

        try {
            new reverseTcpClient().startClient(serverIp, serverPort, Lmin, Lmax, filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // 启动客户端 启动两个线程
    public void startClient(String serverIp, int serverPort, int Lmin, int Lmax, String filePath) throws IOException {
        InetSocketAddress address = new InetSocketAddress(serverIp, serverPort);
        SocketChannel client = SocketChannel.open(address);
        client.configureBlocking(false);
        // 给出文件地址，文件随机分块
        // String filePath = "/Users/lloyd/Desktop/output.txt";
        // 文件保存地址
        String fileName = Paths.get(filePath).getFileName().toString();
        String savePath = "/Users/lloyd/Desktop/reversed_" + fileName + ".txt";
        // 获取分块
        List<byte[]> segments = splitFileData(filePath, Lmin, Lmax);
        // 分块个数
        int N = segments.size();
        // 发送初始化消息
        buffer.putShort(_initialization);
        buffer.putInt(N);
        buffer.flip();
        while(buffer.hasRemaining()){
            client.write(buffer);
        }
        buffer.clear();
        // 等待服务器的agreement————————————————这里的处理有待优化
        int bytesRead = 0;
        while(true){
            bytesRead += client.read(buffer);    // 非阻塞模式下非阻塞，从SocketChannel里面读取数据最小单位就是1个字节，但可能存在一轮读不满预期的情况
            if(bytesRead < 2) continue;

            buffer.flip();                          // limit设为当前position，position回到0
            byte messageType = buffer.get();        // 有四种方法，这种是读取一个字节 缓冲区position + 1
            if(messageType == _agreement){
                System.out.println("System agreed to receive " + N + " segments");
                buffer.clear();
                break;
            }
            buffer.compact();                       // 将缓冲区中未读的数据(position到limit之间)移到缓冲区的起始位置，然后position设置为未读数据的末尾，limit设置为capacity
            if(buffer.position() == 0) break;       // 如果缓冲区已经全部处理完，就退出
        }

        // 发送分块后的文件块 创建发送线程
        SendThread sendThread = new SendThread(client, segments, buffer);
        // 接收文件块 创建接收线程
        ReceivedThread receivedThread = new ReceivedThread(client, buffer, N, savePath);
        // 启动两个线程
        sendThread.start();
        receivedThread.start();
        // 等待两个线程完成
        try{
            sendThread.join();
            receivedThread.join();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        client.close();
    }
    // 文件随机分块
    public List<byte[]> splitFileData(String filePath, int Lmin, int Lmax) throws IOException{
        byte[] fileData = Files.readAllBytes(Paths.get(filePath));
        Random random = new Random();
        int position = 0;
        int totalLength = fileData.length;
        List<byte[]> segments = new ArrayList<>();

        while(position < totalLength){
            int segmentSize = random.nextInt(Lmax - Lmin + 1) + Lmin;
            if(position + segmentSize > totalLength){
                // 最后一段 就把剩下的拿出来即可
                segmentSize = totalLength - position;
            }
            byte[] segment = new byte[segmentSize];
            System.arraycopy(fileData, position, segment, 0, segmentSize);
            segments.add(segment);
            position += segmentSize;
        }
        return segments;
    }
}
// 发送类
class SendThread extends Thread{
    private final SocketChannel client;
    private final List<byte[]> segments;
    private final ByteBuffer buffer;

    public SendThread(SocketChannel client, List<byte[]> segments, ByteBuffer buffer){
        this.client = client;
        this.segments = segments;
        this.buffer = buffer;
    }

    @Override
    public void run(){
        try{
            for(byte[] segment : segments){
                sendMessage(client, reverseTcpClient._clientToServer, segment);
            }
        } catch(IOException e){
            e.printStackTrace();
        } finally {
            try {
                client.shutdownOutput(); // 关闭输出流
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void sendMessage(SocketChannel client, byte messageType, byte[] messageContent) throws IOException{
        // 发送头部字段 type（2B） + size（4B）
        buffer.putShort(messageType);
        buffer.putInt(messageContent.length);
        buffer.flip();
        while(buffer.hasRemaining()){           // 检查的是position和limit之间的距离
            client.write(buffer);
        }
        buffer.clear();
        // 发送内容
        int bytesWrittern = 0;
        while(bytesWrittern < messageContent.length){
            int remaining = messageContent.length - bytesWrittern;
            int writeSize = Math.min(remaining, buffer.remaining());    // buffer的剩余写入量，容器中还有多少数据没写进去
            buffer.put(messageContent, bytesWrittern, writeSize);       // 从messageContent的offset开始写，写进去length个
            bytesWrittern += writeSize;
            buffer.flip();
            while(buffer.hasRemaining()){
                client.write(buffer);
            }
            buffer.clear();
        }
    }
}
// 接收类
class ReceivedThread extends Thread{
    private final SocketChannel client;
    private final ByteBuffer buffer;
    private final ByteBuffer headerBuffer;
    private int N;
    private final String savePath;

    public ReceivedThread(SocketChannel client, ByteBuffer buffer, int N, String savePath) {
        this.client = client;
        this.buffer = buffer;
        this.N = N;
        this.savePath = savePath;
        headerBuffer = ByteBuffer.allocate(reverseTcpClient._headerSize);
    }

    @Override
    public void run() {
        Stack<byte[]> receivedSegments = new Stack<>();
        try{
            while (true) {
                byte[] messageContent = new byte[0];                      // 准备数据
                // 先读取头部6字节
                while(headerBuffer.hasRemaining()){
                    client.read(headerBuffer);
                }
                headerBuffer.flip();
                byte[] messageType = new byte[2];
                headerBuffer.get(messageType);
                int segmentSize = headerBuffer.getInt();
                headerBuffer.clear();
                if(messageType[1] == reverseTcpClient._serverToClient){
                    messageContent = new byte[segmentSize];
                    System.out.println("Receiving segment (Size: " + segmentSize + " )from Server...");
                }
                // 读取剩余内容
                int bytesRead = 0;              // 每轮开始前的已经读取的长度
                while(bytesRead < segmentSize){
                    if (bytesRead == -1) {
                        System.out.println("Server closed connection");
                        break;
                    }
                    int leftBytesToRead = Math.min(buffer.remaining(), segmentSize - bytesRead);
                    buffer.limit(leftBytesToRead);
                    int bytesReadLength = client.read(buffer);
                    if(bytesReadLength == 0) continue;
                    buffer.flip();
                    buffer.get(messageContent, bytesRead, bytesReadLength);
                    bytesRead += bytesReadLength;
                    buffer.clear();
                }
                String receivedString = new String(messageContent);
                System.out.println("Received reversed String: " + receivedString);
                receivedSegments.push(messageContent);
                if(--N == 0) break;
            }
            // 全部数据都读完，保存到指定文件中
            saveToFile(receivedSegments, savePath);
        } catch(IOException e){
            e.printStackTrace();
        } finally{
            try {
                client.shutdownInput(); // 关闭输入流
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    // 保存反转数据到文件
    public void saveToFile(Stack<byte[]> segments, String filePath) throws IOException{

        try{
            Path path = Paths.get(filePath);
            // 创建文件输出流
            FileOutputStream fos = new FileOutputStream(path.toFile());
            while(!segments.isEmpty()){
                byte[] segment = segments.pop();
                fos.write(new String(segment).getBytes());
            }
            fos.close();
            System.out.println("File saved succeed!");
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
