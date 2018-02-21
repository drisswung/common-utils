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
 */
public class DelayDial {
	
	private static final Logger logger = LoggerFactory.getLogger(DelayDial.class);

	private int secondsPerLoop;
	
	private Slot[] loop;
	
	private int currentIndex;
	
	private Thread dialThread;
	
	private ExecutorService workers;
	
	private AtomicBoolean shutDown = new AtomicBoolean(false);
	
	/**
	 * @param secondsPerLoop 多少秒一圈
	 * @param workerThreads 工作线程数
	 */
	public DelayDial(int secondsPerLoop, int workerThreads) {
		
		if (secondsPerLoop <= 0) {
			throw new IllegalArgumentException("seconds per loop must not be minus");
		}
		this.secondsPerLoop = secondsPerLoop;
		init(workerThreads);
	}
	
	public void commitTask(int delaySeconds, Runnable runnable) {
		
		if (shutDown.get()) {
			throw new RuntimeException("delay dial already have be shuted down.");
		}
		int circle = delaySeconds / secondsPerLoop;
		int slotIndex = currentIndex + (delaySeconds - circle * secondsPerLoop);
		if (slotIndex >= secondsPerLoop) {
			slotIndex = slotIndex - secondsPerLoop;
		}
		Task task = new Task(circle, runnable);
		Slot slot = loop[slotIndex];
		slot.queue.add(task);
		
	}
	
	public void shutDownNow() {
		
		shutDown.set(true);
		this.workers.shutdownNow();
	}
	
	private void init(int workerThreads) {
		
		this.loop = new Slot[this.secondsPerLoop];
		for (int i = 0; i < this.secondsPerLoop; i++) {
			loop[i] = new Slot(i);
			if (i == 0) continue;
			
			loop[i].prefix = loop[i - 1];
			loop[i - 1].next = loop[i];
			
			if (i == secondsPerLoop -1) {
				loop[i].next = loop[0];
				loop[0].prefix = loop[i];
			}
		}
		currentIndex = 0;
		this.workers = new ThreadPoolExecutor(workerThreads, workerThreads, 0, TimeUnit.SECONDS, 
											  new LinkedBlockingQueue<Runnable>(),
											  new DelayDialThreadFactory(),
											  new ThreadPoolExecutor.AbortPolicy());
		logger.info("init delaydial, size of workers:[{}]", workerThreads);
	}
	
	public void start() {
		
		dialThread = new Thread(() -> {
			while (!shutDown.get()) {
				
				long start = System.currentTimeMillis();
				
				Slot slot = loop[currentIndex];
				handle(slot);
				
				// 一般为 0~1毫秒
				logger.info("dial cost:[{}]", System.currentTimeMillis() - start);
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
				
				currentIndex++;
				if (currentIndex >= secondsPerLoop) {
					currentIndex = 0;
				}
			}
		}, "dial-thread");
		dialThread.start();
	}
	
	private void handle(Slot slot) {
		SlotProcessor processor = new SlotProcessor(slot, workers);
		workers.submit(processor);
	}
	
	static class SlotProcessor implements Runnable {
		
		private Slot slot;
		private ExecutorService executorService;
		
		public SlotProcessor(Slot slot, ExecutorService executorService) {
			this.slot = slot;
			this.executorService = executorService;
		}

		@Override
		public void run() {
			if (slot == null) return;
			
			long start = System.currentTimeMillis();
			
			Queue<Task> queue = slot.queue;
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
	
	static class Slot {
		
		private int no;
		
		private Slot prefix;
		
		private Slot next;

		private Queue<Task> queue = new LinkedBlockingQueue<>();

		public Slot(int no) {
			super();
			this.no = no;
		}
		
		@Override
		public String toString() {
			return "Slot [no=" + no + ", prefix=" + prefix.no + ", next=" + next.no + "]";
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
