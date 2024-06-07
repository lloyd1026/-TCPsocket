import java.io.IOException;
import java.io.File;
import java.io.RandomAccessFile;

import java.net.InetSocketAddress;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
            System.out.println("第6个参数可选[反转文件保存路径] 不提供采用默认地址");
            return;
        }
        String serverIp = args[0];
        int serverPort = Integer.parseInt(args[1]);
        // 随机分段的长度限定范围，最后一块除外（最后一块可能会小于Lmin）
        int Lmin = Integer.parseInt(args[2]);
        int Lmax = Integer.parseInt(args[3]);
        String filePath = args[4];
        String savePath = null;

        if (args.length >= 6){
            savePath = args[5];
        }

        try {
            new reverseTcpClient().startClient(serverIp, serverPort, Lmin, Lmax, filePath, savePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 启动客户端 启动两个线程
    public void startClient(String serverIp, int serverPort, int Lmin, int Lmax, String filePath, String savePath) throws IOException {
        InetSocketAddress address = new InetSocketAddress(serverIp, serverPort);
        SocketChannel client = SocketChannel.open(address);
        client.configureBlocking(false);

        // 文件保存地址
        if(savePath == null){
            Path path = Paths.get(filePath);
            String fileName = path.getFileName().toString();
            String parentDir = path.getParent().toString();
            savePath = Paths.get(parentDir, "reversed_" + fileName).toString();
//            String fileName = Paths.get(filePath).getFileName().toString();
//            savePath = "/Users/lloyd/Desktop/reversed_" + fileName + ".txt";
        }

        // 获取分块
        List<FileSegment> segments = splitFileData(filePath, Lmin, Lmax);
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
        ByteBuffer typeBuffer = ByteBuffer.allocate(2);
        while(typeBuffer.hasRemaining()){
            client.read(typeBuffer);    // 非阻塞模式下非阻塞，从SocketChannel里面读取数据最小单位就是1个字节，但可能存在一轮读不满预期的情况
        }
        typeBuffer.flip();                          // limit设为当前position，position回到0
        short messageType = typeBuffer.getShort();
        if(messageType != _agreement){
            System.out.println("Server refuse your request");
            return;
        } else{
            System.out.println("Server agreed to receive " + N + " segments");
            typeBuffer.clear();
        }

        // 发送分块后的文件块 创建发送线程
        SendThread sendThread = new SendThread(client, filePath, segments);
        // 接收文件块 创建接收线程
        ReceivedThread receivedThread = new ReceivedThread(client, N, savePath);
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
    public List<FileSegment> splitFileData(String filePath, int Lmin, int Lmax) throws IOException{
        List<FileSegment> segments = new ArrayList<>();
        Path path = Paths.get(filePath);
        long fileSize = Files.size(path);
        long position = 0;

        Random random = new Random();

        while (position < fileSize) {
            int segmentSize = random.nextInt(Lmax - Lmin + 1) + Lmin;
            if (position + segmentSize > fileSize) {
                segmentSize = (int) (fileSize - position);
            }
            segments.add(new FileSegment(position, segmentSize));
            position += segmentSize;
        }
        return segments;
    }

    static class FileSegment{
        long position;
        int size;
        public FileSegment(long position, int size){
            this.position = position;
            this.size = size;
        }
    }
}
// 发送类
class SendThread extends Thread{
    private final SocketChannel client;
    private final List<reverseTcpClient.FileSegment> segments;
    private final ByteBuffer buffer;
    private final String filePath;

    public SendThread(SocketChannel client, String filePath, List<reverseTcpClient.FileSegment> segments){
        this.client = client;
        this.filePath = filePath;
        this.segments = segments;
        buffer = ByteBuffer.allocate(1024);
    }

    @Override
    public void run(){
        try{
            for(reverseTcpClient.FileSegment segment : segments){
                sendMessage(client, reverseTcpClient._clientToServer, filePath, segment);
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

    public void sendMessage(SocketChannel client, byte messageType, String filePath, reverseTcpClient.FileSegment segment) throws IOException{
        // 发送头部字段 type（2B） + size（4B）
        buffer.putShort(messageType);
        buffer.putInt(segment.size);
        buffer.flip();
        while(buffer.hasRemaining()){           // 检查的是position和limit之间的距离
            client.write(buffer);
        }
        buffer.clear();
        // 发送内容
        try (FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
            fileChannel.position(segment.position);
            while (segment.size > 0) {
                int leftBytesToRead = Math.min(buffer.remaining(), segment.size);
                buffer.limit(leftBytesToRead);
                int bytesRead = fileChannel.read(buffer);
                if (bytesRead == -1) break;
                buffer.flip();
                while (buffer.hasRemaining()) {
                    client.write(buffer);
                }
                buffer.clear();
                segment.size -= bytesRead;
            }
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
    private final String tempFolderPath = "/Users/lloyd/temp";

    public ReceivedThread(SocketChannel client, int N, String savePath) {
        this.client = client;
        buffer = ByteBuffer.allocate(1024);
        this.N = N;
        this.savePath = savePath;
        headerBuffer = ByteBuffer.allocate(reverseTcpClient._headerSize);
    }

    @Override
    public void run() {
        try{
            while (true) {
                byte[] messageContent = new byte[0];                      // 准备数据
                // 先读取头部6字节
                while(headerBuffer.hasRemaining()){
                    client.read(headerBuffer);
                }
                headerBuffer.flip();

                short messageType = headerBuffer.getShort();
                int segmentSize = headerBuffer.getInt();
                headerBuffer.clear();
                if(messageType == reverseTcpClient._serverToClient){
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

                insertDataAtFileHead(savePath, messageContent);

                if(--N == 0) break;
            }
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
    // 保存文件
    public static void insertDataAtFileHead(String filePath, byte[] segment) throws IOException{
        File file = new File(filePath);
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        long originalLength = raf.length();

        File tempFile = new File(filePath + ".tmp");
        RandomAccessFile tempRaf = new RandomAccessFile(tempFile, "rw");
        // 将待插入数据写入临时文件
        tempRaf.write(segment);

        raf.seek(0);
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = raf.read(buffer)) != -1) {
            tempRaf.write(buffer, 0, bytesRead);
        }

        tempRaf.close();
        raf.close();

        if (!file.delete()) {
            throw new IOException("Failed to delete the original file");
        }
        if (!tempFile.renameTo(file)) {
            throw new IOException("Failed to rename the temporary file to the original file");
        }
    }
}
