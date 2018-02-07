package go.driss.pn.timer;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 延时轮盘
 * @author yiyun_wang
 *
 * @param <T>
 * @param <R>
 */
public class DelayDial {
	
	private static final Logger logger = LoggerFactory.getLogger(DelayDial.class);

	private int secondsPerLoop;
	
	private Node[] loop;
	
	private int currentIndex;
	
	private Thread dialThread;
	
	private ExecutorService workers;
	
	private AtomicBoolean shutDown = new AtomicBoolean(false);
	
	public DelayDial(int secondsPerLoop, int workerThreads) {
		
		if (secondsPerLoop <= 0) {
			throw new IllegalArgumentException("seconds per loop must not be minus");
		}
		this.secondsPerLoop = secondsPerLoop;
		init(workerThreads);
		start();
	}
	
	public void commitTask(int delaySeconds, Runnable runnable) {
		
		if (shutDown.get()) {
			throw new RuntimeException("delay dial already have be shuted down.");
		}
		int circle = delaySeconds / secondsPerLoop;
		int slot = currentIndex + (delaySeconds - circle * secondsPerLoop);
		if (slot >= secondsPerLoop) {
			slot = slot - secondsPerLoop;
		}
		Task task = new Task(circle, runnable);
		Node node = loop[slot];
		node.queue.add(task);
		
	}
	
	public void shutDown() {
		
		shutDown.set(true);
		for (Node node : loop) {
			if (node.queue.isEmpty()) {
				continue;
			}
		}
		this.workers.shutdown();
		try {
			while (!this.workers.awaitTermination(1, TimeUnit.SECONDS)) {
				continue;
			}
		} catch (InterruptedException e) {}
	}
	
	public void shutDownNow() {
		
		shutDown.set(true);
		this.workers.shutdownNow();
	}
	
	public static void main(String[] args) throws Exception {
		
		
		DelayDial delayDial = new DelayDial(60, 4);
		long start = System.currentTimeMillis();
		delayDial.commitTask(6, () -> {
			System.out.println((System.currentTimeMillis() - start) / 1000);
		});
		delayDial.commitTask(7, () -> System.out.println((System.currentTimeMillis() - start) / 1000));
		delayDial.commitTask(8, () -> System.out.println((System.currentTimeMillis() - start) / 1000));
		delayDial.commitTask(23, () -> System.out.println((System.currentTimeMillis() - start) / 1000));
		delayDial.commitTask(67, () -> System.out.println((System.currentTimeMillis() - start) / 1000));
		delayDial.commitTask(129, () -> System.out.println((System.currentTimeMillis() - start) / 1000));
	}

	private void init(int workerThreads) {
		
		this.loop = new Node[this.secondsPerLoop];
		for (int i = 0; i < this.secondsPerLoop; i++) {
			loop[i] = new Node(i);
			if (i == 0) continue;
			
			loop[i].prefix = loop[i - 1];
			loop[i - 1].next = loop[i];
			
			if (i == secondsPerLoop -1) {
				loop[i].next = loop[0];
				loop[0].prefix = loop[i];
			}
		}
		currentIndex = 0;
		this.workers = new ThreadPoolExecutor(0, workerThreads, 5, TimeUnit.MINUTES, 
											  new LinkedBlockingQueue<Runnable>(),
											  new DelayDialThreadFactory(),
											  new ThreadPoolExecutor.AbortPolicy());
		logger.info("init delaydial, size of workers:[{}]", workerThreads);
	}
	
	private void start() {
		
		dialThread = new Thread(() -> {
			while (!shutDown.get()) {
				
				long start = System.currentTimeMillis();
				
				Node node = loop[currentIndex];
				handle(node);
				currentIndex++;
				if (currentIndex >= secondsPerLoop) {
					currentIndex = 0;
				}
				
				// 一般为 0~1毫秒
				logger.info("dial cost:[{}]", System.currentTimeMillis() - start);
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
			}
		}, "dial-thread");
		dialThread.start();
	}
	
	private void handle(Node node) {
		NodeProcessor processor = new NodeProcessor(node, workers);
		workers.submit(processor);
	}
	
	static class NodeProcessor implements Runnable {
		
		private Node node;
		private ExecutorService executorService;
		
		public NodeProcessor(Node node, ExecutorService executorService) {
			this.node = node;
			this.executorService = executorService;
		}

		@Override
		public void run() {
			if (node == null) return;
			
			long start = System.currentTimeMillis();
			
			Queue<Task> queue = node.queue;
			Iterator<Task> iterator = queue.iterator();
			while (iterator.hasNext()) {
				Task task = iterator.next();
				if (task.circle > 0) {
					task.circle--;
					continue;
				}
				Runnable runnable = task.runnable;
				executorService.submit(runnable);
				iterator.remove();
			}
			
			long cost = System.currentTimeMillis() - start;
			logger.info("process slot, cost:[{}], remaining:[{}] ", cost, !queue.isEmpty());
		}
	}
	
	static class Task {
		
		int circle;
		Runnable runnable;
		
		public Task(int circle, Runnable runnable) {
			super();
			this.circle = circle;
			this.runnable = runnable;
		}
	}
	
	private static class Node {
		
		int no;
		
		Node prefix;
		
		Node next;

		private Queue<Task> queue = new LinkedBlockingQueue<>();

		public Node(int no) {
			super();
			this.no = no;
		}
		
		@Override
		public String toString() {
			return "Node [no=" + no + ", prefix=" + prefix.no + ", next=" + next.no + "]";
		}
	}
	
	static class DelayDialThreadFactory implements ThreadFactory {
		
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DelayDialThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                                  Thread.currentThread().getThreadGroup();
            namePrefix = "delayDialPool-" +
                          poolNumber.getAndIncrement() +
                         "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                                  namePrefix + threadNumber.getAndIncrement(),
                                  0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
	
}
