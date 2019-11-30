import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/***
 * 简单实现一个基于IO的多线程队列引擎
 * 多个生产者共同发送messageSendNum条消息，均匀有序地发到各个queue中
 * 多个消费者各自随机地校验10%的queue，确保消息有序并且数量没有缺失
 * 发送阶段完毕之后再开始校验阶段
 * 5个地方填空
 */
public class MultiThreading {

	public static Collection<byte[]> EMPTY = new ArrayList<byte[]>();

	public static void main(String[] args) throws Exception {
		// 队列的数量
		int queueNum = 100;
		// 生产者总的消息发送量
		int messageSendNum = 20 * 10000;
		// 消费者校验的次数
		int checkQueueNum = queueNum/10;

		long startTime = System.currentTimeMillis();

		ConcurrentMap<String, AtomicInteger> queueNumMap = new ConcurrentHashMap<>();
		for (int i = 0; i < queueNum; i++) {
			queueNumMap.put("Queue-" + i, new AtomicInteger(0));
		}

		// 生产者
		int sendTsNum = 4;
		// 当前发送消息计数器
		AtomicLong sendCounter = new AtomicLong(0);
		QueueStore queueStore = new QueueStore();
		Thread[] sends = new Thread[sendTsNum];
		for (int i = 0; i < sendTsNum; i++) {
			sends[i] = new Thread(new Producer(queueStore, messageSendNum, sendCounter, queueNumMap));
		}
		for (int i = 0; i < sendTsNum; i++) {
			// 启动线程
			//**************************
			sends[i].start();
			//**************************
		}
		for (int i = 0; i < sendTsNum; i++) {
			//保证在消费者开始之前，结束生产者任务
			//**************************
			sends[i].join();
			//**************************
		}
		System.out.println("End of production process");
		// 消费者
		int checkTsNum = 4;
		Thread[] checks = new Thread[checkTsNum];
		for (int i = 0; i < checkTsNum; i++) {
			checks[i] = new Thread(new Consumer(queueStore,  checkQueueNum, queueNumMap));
		}
		for (int i = 0; i < checkTsNum; i++) {
			checks[i].start();
		}
		for (int i = 0; i < checkTsNum; i++) {
			//保证在统计运行时间开始之前，结束消费者任务
			//**************************
			checks[i].join();
			//**************************
		}
		System.out.println("End of consumption process");
		long endTime = System.currentTimeMillis();
		// 统计运行时间
		System.out.printf("cost time : [%.2f] s", (endTime - startTime + 0.1) / 1000);
	}
}

class QueueStore {

	public void put(String queueName, byte[] message) throws IOException {
		File dir = new File("data");
		if (!dir.exists()) {
			dir.mkdirs();
		}
		File file = new File("data/" + queueName);
		if (!file.exists()) {
			file.createNewFile();
		}
		// 将消息写入到指定queue文件
		FileOutputStream out = new FileOutputStream(file,true);
		out.write(message.length);
		out.write(message);
		out.close();
	}

	public Collection<byte[]> get(String queueName, long offset, long num) throws NumberFormatException, IOException {
		File file = new File("data/" + queueName);
		if (!file.exists()) {
			return MultiThreading.EMPTY;
		}
		Collection<byte[]> result = new ArrayList<>();
		// 从指定queue文件中读取一系列的消息
		FileInputStream in = new FileInputStream(file);
		int msgLen = 0;
		while ((msgLen = in.read()) > 0 && num > 0) {
			byte[] byteArr = new byte[msgLen];
			in.read(byteArr);
			String msg = new String(byteArr);
			if (Long.valueOf(msg.split(" ")[1]) == offset) {
				result.add(byteArr);
				num--;
				offset++;
			}
		}
		in.close();
		return result;
	}
}

class Producer implements Runnable {

	QueueStore queueStore;
	int messageSendNum;
	AtomicLong sendCounter;
	ConcurrentMap<String, AtomicInteger> queueNumMap;

	public Producer(QueueStore queueStore, int messageSendNum, AtomicLong sendCounter,
					ConcurrentMap<String, AtomicInteger> queueNumMap) {
		//初始化成员变量
		//**************************
		this.queueStore = queueStore;
		this.messageSendNum = messageSendNum;
		this.sendCounter = sendCounter;
		this.queueNumMap = queueNumMap;
		//**************************
	}

	@Override
	public void run() {
		long count;
		while ((count = sendCounter.getAndIncrement()) < messageSendNum) {
			try {
				String queueName = "Queue-" + count % queueNumMap.size();
				// 同步代码块，竞争每个queueNumMap中的AtomicInteger对象锁
				//**************************
				synchronized (AtomicInteger.class) {
					queueStore.put(queueName,
							(queueName + " " + queueNumMap.get(queueName).getAndIncrement()).getBytes());
				}
				//**************************
			} catch (Throwable t) {
				t.printStackTrace();
				System.exit(-1);
			}
		}
	}
}

class Consumer implements Runnable {
	QueueStore queueStore;
	int checkQueueNum;
	AtomicLong checkTimeCounter;
	ConcurrentMap<String, AtomicInteger> queueNumMap;
	public Consumer(QueueStore queueStore, int checkQueueNum,  ConcurrentMap<String, AtomicInteger> queueNumMap) {
		this.queueStore = queueStore;
		this.checkQueueNum = checkQueueNum;
		this.queueNumMap = queueNumMap;
	}

	@Override
	public void run() {
		Random random = new Random();
		int queueNum = queueNumMap.size();
		ConcurrentMap<String, AtomicInteger> offsets = new ConcurrentHashMap<>();
		//从queueNumMap中随机抽取不重复的10%queue放入offsets中
		for (int j = 0; j < checkQueueNum; j++) {
			String queueName = "Queue-" + random.nextInt(queueNum);
			while (offsets.containsKey(queueName)) {
				queueName = "Queue-" + random.nextInt(queueNum);
			}
			offsets.put(queueName, queueNumMap.get(queueName));
		}
		ConcurrentMap<String, AtomicInteger> pullOffsets = new ConcurrentHashMap<>();
		for (String queueName: offsets.keySet()) {
			pullOffsets.put(queueName, new AtomicInteger(0));
		}
		while (pullOffsets.size() > 0) {
			try {
				for (String queueName : pullOffsets.keySet()) {
					int index = pullOffsets.get(queueName).get();
					Collection<byte[]> msgs = queueStore.get(queueName, index, 10);
					if (msgs != null && msgs.size() > 0) {
						pullOffsets.get(queueName).getAndAdd(msgs.size());
						for (byte[] msg : msgs) {
							// 校验消息顺序是否有序
							String id = new String(msg).split(" ")[1];
							if (!id.equals(String.valueOf(index++))) {
								System.out.printf("Consume Check error:get %s should be %d",id,index-1);
								System.exit(-1);
							}
						}
					}
					//读到最后，统计消费数量是否正确
					if (msgs == null || msgs.size() < 10) {
						if (pullOffsets.get(queueName).get() != offsets.get(queueName).get()) {
							System.out.printf(pullOffsets.get(queueName).get()+":"+offsets.get(queueName).get());
							System.out.printf("Queue Number Error");
							System.exit(-1);
						}
						pullOffsets.remove(queueName);
					}
				}
			} catch (Throwable t) {
				t.printStackTrace();
				System.exit(-1);
			}
		}

	}

}
