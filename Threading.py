import threading
import time  # For demonstration delays and monotonic time


class CoordinatedThreads:
    """
    Manages and synchronizes a main thread and multiple worker threads.
    
    Each thread can be configured to run its function once or in a loop
    for a specified duration.
    
    All worker threads initialize first. The main thread waits for all workers
    to report initialization. Once all workers are initialized, the main thread
    signals all threads to proceed with their assigned tasks.
    
    Threads can be grouped into "sync groups" where looping threads will synchronize
    between iterations, ensuring all threads in the group complete one iteration before
    any proceed to the next.
    """
    def __init__(self, thread_configs):
        """
        Initializes the coordinator with thread configurations.
        
        Args:
            thread_configs (list): A list of dictionaries. The first dictionary
                configures the main thread, subsequent dictionaries configure
                worker threads. Each dictionary must contain:
                - 'func' (callable): The function for the thread.
                - 'loop_duration' (float, optional): If provided and > 0,
                  the function will be called in a loop for this many seconds.
                - 'sync_group' (str, optional): If provided, the thread will 
                  synchronize iterations with other threads in the same group.
                  Only meaningful for looping threads.
        """
        if not isinstance(thread_configs, list) or not thread_configs:
            raise ValueError("thread_configs must be a non-empty list of configuration dictionaries.")
        
        # First config is for the main thread
        main_config = thread_configs[0]
        if not isinstance(main_config, dict) or 'func' not in main_config:
            raise TypeError("Main thread config must be a dict with at least a 'func' key.")
        if not callable(main_config['func']):
            raise TypeError("Main thread 'func' must be callable.")
        self.main_config = main_config
        if 'loop_duration' in main_config and (not isinstance(main_config['loop_duration'], (int, float)) or main_config['loop_duration'] < 0):
            raise TypeError("Main thread 'loop_duration' must be a non-negative number (int or float).")
        
        # Rest are for worker threads
        self.worker_configs = []
        worker_thread_configs = thread_configs[1:]
        for i, config in enumerate(worker_thread_configs):
            if not isinstance(config, dict) or 'func' not in config:
                raise TypeError(f"Worker config at index {i} must be a dict with at least a 'func' key.")
            if not callable(config['func']):
                raise TypeError(f"Worker 'func' at index {i} must be callable.")
            if 'loop_duration' in config and (not isinstance(config['loop_duration'], (int, float)) or config['loop_duration'] < 0):
                raise TypeError(f"Worker 'loop_duration' at index {i} must be a non-negative number (int or float).")
            self.worker_configs.append(config)
        
        self.num_workers = len(self.worker_configs)
        
        # Synchronization primitives
        self.init_condition = threading.Condition()
        self.initialized_workers_count = 0
        self.all_ready_event = threading.Event()
        
        # Initialize sync group management
        self.sync_groups = {}  # Maps sync_group name -> barrier
        self.sync_group_members = {}  # Maps sync_group name -> count of members
        
        # Count threads in each sync group to create appropriate barriers
        all_configs = [main_config] + self.worker_configs
        for config in all_configs:
            if 'sync_group' in config and config.get('loop_duration', 0) > 0:
                group_name = config['sync_group']
                if group_name not in self.sync_group_members:
                    self.sync_group_members[group_name] = 0
                self.sync_group_members[group_name] += 1
        
        # Create barriers for each sync group
        for group_name, count in self.sync_group_members.items():
            self.sync_groups[group_name] = threading.Barrier(count)
            print(f"Created sync group '{group_name}' with {count} threads")
        
        self.threads = []
    
    def _execute_user_task(self, thread_num, thread_label, config):
        """Helper to execute the user's function, handling looping and synchronization."""
        user_func = config['func']
        loop_duration = config.get('loop_duration')  # Defaults to None if not present
        sync_group = config.get('sync_group')  # Get sync group if specified
        
        # Check if thread is part of a sync group
        is_synced = sync_group in self.sync_groups and loop_duration is not None and loop_duration > 0
        sync_barrier = self.sync_groups.get(sync_group) if is_synced else None
        
        if loop_duration is not None and loop_duration > 0:
            print(f"Thread {thread_num} ({thread_label}) starting user task (looping for {loop_duration}s)...")
            if is_synced:
                print(f"Thread {thread_num} ({thread_label}) is part of sync group '{sync_group}'")
                
            start_time = time.monotonic()
            loop_count = 0
            while time.monotonic() - start_time < loop_duration:
                loop_count += 1
                try:
                    user_func(thread_num)
                except Exception as e:
                    print(f"Thread {thread_num} ({thread_label}) Error during user function (in loop iteration {loop_count}): {e}")
                    break  # Exit loop on error in user function
                
                # If part of a sync group, wait for other threads before next iteration
                if is_synced:
                    print(f"Thread {thread_num} ({thread_label}) waiting at sync point (iteration {loop_count})")
                    try:
                        sync_barrier.wait()
                        print(f"Thread {thread_num} ({thread_label}) passed sync point (iteration {loop_count})")
                    except threading.BrokenBarrierError:
                        print(f"Thread {thread_num} ({thread_label}) sync barrier was broken")
                        break
            
            elapsed_time = time.monotonic() - start_time
            print(f"Thread {thread_num} ({thread_label}) finished user task loop (ran for approx {elapsed_time:.2f}s, {loop_count} iterations).")
        else:
            print(f"Thread {thread_num} ({thread_label}) starting user task (single run)...")
            try:
                user_func(thread_num)
            except Exception as e:
                print(f"Thread {thread_num} ({thread_label}) Error during user function (single run): {e}")
            print(f"Thread {thread_num} ({thread_label}) finished user task (single run).")
    
    def _main_orchestrator(self, thread_num, config):
        """Orchestrator for the main thread."""
        print(f"Thread {thread_num} (Main Orchestrator) initializing...")
        
        if self.num_workers > 0:
            with self.init_condition:
                print(f"Thread {thread_num} (Main Orchestrator) waiting for {self.num_workers} workers...")
                while self.initialized_workers_count < self.num_workers:
                    self.init_condition.wait()
                print(f"Thread {thread_num} (Main Orchestrator): All {self.num_workers} workers initialized.")
        else:
            print(f"Thread {thread_num} (Main Orchestrator): No workers to wait for.")
        
        print(f"Thread {thread_num} (Main Orchestrator): Signaling proceed.")
        self.all_ready_event.set()
        
        # Execute user's task for the main thread
        self._execute_user_task(thread_num, "Main", config)
        print(f"Thread {thread_num} (Main Orchestrator) completed its phase.")
    
    def _worker_orchestrator(self, thread_num, config):
        """Orchestrator for worker threads."""
        print(f"Thread {thread_num} (Worker Orchestrator) initializing...")
        
        with self.init_condition:
            self.initialized_workers_count += 1
            print(f"Thread {thread_num} (Worker Orchestrator) signaled init. Count: {self.initialized_workers_count}")
            self.init_condition.notify()
        
        print(f"Thread {thread_num} (Worker Orchestrator) waiting for proceed signal...")
        self.all_ready_event.wait()
        
        # Execute user's task for this worker thread
        self._execute_user_task(thread_num, f"Worker", config)  # Simplified label for worker
        print(f"Thread {thread_num} (Worker Orchestrator) completed its phase.")
    
    def run(self):
        """Creates, starts, and joins all threads based on configurations."""
        total_threads = 1 + self.num_workers
        print(f"Starting {total_threads} threads ({1} main, {self.num_workers} workers)...")
        self.threads = []  # Reset threads list for potential re-runs
        self.initialized_workers_count = 0  # Reset for re-runs
        self.all_ready_event.clear()  # Reset for re-runs
        
        # Reset barriers for sync groups
        for group_name, count in self.sync_group_members.items():
            self.sync_groups[group_name] = threading.Barrier(count)
        
        # Create main thread (thread 0)
        t_main = threading.Thread(
            target=self._main_orchestrator,
            args=(0, self.main_config),  # Pass main config
            name="MainThread"
        )
        self.threads.append(t_main)
        
        # Create worker threads (threads 1 to num_workers)
        for i, worker_cfg in enumerate(self.worker_configs):
            worker_num = i + 1
            t_worker = threading.Thread(
                target=self._worker_orchestrator,
                args=(worker_num, worker_cfg),  # Pass worker config
                name=f"WorkerThread-{worker_num}"
            )
            self.threads.append(t_worker)
        
        # Start all threads
        for t in self.threads:
            t.start()
        
        # Wait for all threads to complete
        for t in self.threads:
            t.join()
        
        print("All threads finished.")

# --- Example Usage ---

def my_main_thread_work(thread_num):
    print(f" -----> Thread {thread_num} (Custom Main) performing its action. Timestamp: {time.time():.2f}")
    time.sleep(0.7)  # Simulate work

def custom_work_for_worker_one(thread_num):
    print(f" -----> Thread {thread_num} (Custom Worker 1) doing Task Alpha! Timestamp: {time.time():.2f}")
    time.sleep(0.5)

def custom_work_for_worker_two(thread_num):
    print(f" -----> Thread {thread_num} (Custom Worker 2) executing Task Beta. Timestamp: {time.time():.2f}")
    time.sleep(1.0)

def custom_work_for_worker_three(thread_num):
    print(f" -----> Thread {thread_num} (Custom Worker 3) running Task Gamma. Timestamp: {time.time():.2f}")
    time.sleep(0.3)


# --- Main Execution Block ---
if __name__ == "__main__":
    # Define thread configurations with sync groups
    # Main thread and Worker 1 will sync together (group "A")
    # Worker 2 and Worker 3 will sync together (group "B")
    thread_configurations = [
        {'func': my_main_thread_work, 'loop_duration': 3.0, 'sync_group': 'A'},  # Main thread
        {'func': custom_work_for_worker_one, 'loop_duration': 3.0, 'sync_group': 'A'},  # Worker 1
        {'func': custom_work_for_worker_two, 'loop_duration': 3.0, 'sync_group': 'B'},  # Worker 2
        {'func': custom_work_for_worker_three, 'loop_duration': 3.0, 'sync_group': 'B'}  # Worker 3
    ]
    
    try:
        print("--- Scenario 1: Multiple workers with sync groups ---")
        coordinator = CoordinatedThreads(thread_configs=thread_configurations)
        coordinator.run()
        print("Done with Scenario 1.\n")
        
        print("--- Scenario 2: One sync group ---")
        single_sync_group_config = [
            {'func': my_main_thread_work, 'loop_duration': 2.0, 'sync_group': 'C'},
            {'func': custom_work_for_worker_one, 'loop_duration': 2.0, 'sync_group': 'C'},
            {'func': custom_work_for_worker_two, 'loop_duration': 2.0, 'sync_group': 'C'},
        ]
        coordinator_single_group = CoordinatedThreads(thread_configs=single_sync_group_config)
        coordinator_single_group.run()
        print("Done with Scenario 2.\n")
        
        print("--- Scenario 3: Mixed sync and non-sync threads ---")
        mixed_config = [
            {'func': my_main_thread_work, 'loop_duration': 2.0, 'sync_group': 'D'},
            {'func': custom_work_for_worker_one, 'loop_duration': 2.0, 'sync_group': 'D'},
            {'func': custom_work_for_worker_two},  # Runs once, no sync
            {'func': custom_work_for_worker_three, 'loop_duration': 2.0}  # Loops but no sync
        ]
        coordinator_mixed = CoordinatedThreads(thread_configs=mixed_config)
        coordinator_mixed.run()
        print("Done with Scenario 3.\n")
        
    except (ValueError, TypeError) as e:
        print(f"Error setting up or running threads: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")