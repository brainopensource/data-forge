"""
Asynchronous Task Runner for Tkinter
"""
import threading
import queue
from typing import Any, Callable, Optional

class AsyncRunner:
	def __init__(self, tk_root: Any):
		self.tk_root = tk_root
		self.queue = queue.Queue()
		self.tk_root.after(100, self._process_queue)

	def run(self, fn: Callable, on_done: Optional[Callable] = None, on_error: Optional[Callable] = None):
		"""Runs a function in a background thread."""
		def thread_fn():
			try:
				result = fn()
				if on_done:
					self.queue.put((on_done, result))
			except Exception as e:
				if on_error:
					self.queue.put((on_error, e))

		thread = threading.Thread(target=thread_fn)
		thread.daemon = True
		thread.start()

	def _process_queue(self):
		"""Processes results from the queue in the main Tkinter thread."""
		try:
			while not self.queue.empty():
				callback, arg = self.queue.get_nowait()
				callback(arg)
		finally:
			self.tk_root.after(100, self._process_queue)
