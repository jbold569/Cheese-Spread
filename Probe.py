import Queue
import time as t
import threading

class Worker(threading.Thread):
	def __init__(self, parameters):
		threading.Thread.__init__(self)
		self.measurements = parameters[0]
		self.message = parameters[1]
		self.delay = parameters[2]
	
	def run(self):
		while True:
			start = t.time()
			total = 0.0
			counter = 0
			while t.time() - start < self.delay:
				total += self.measurements.get()
				counter += 1
			average = total/counter
			print self.message % (1/average)
	
	
class Probe:
	def __init__(self):
		# Structure {id: (queue, message, delay), ...}
		self.probe_parameters = {}
		# Structure {id: start_times}
		self.start_times = {}
		
	def InitProbe(self, id, message, delay):
			if id in self.probe_parameters.keys():
				print "ID already in use. Exiting..."
				exit()
			self.probe_parameters[id] = (Queue.Queue(), message, delay)
	
	def RunProbes(self):
		for id, param in self.probe_parameters.items():
			th = Worker(param)
			th.setDaemon(True)
			th.start()
		
	def StartTiming(self, id):
		self.start_times[id] = t.time()
	
	def StopTiming(self, id):
		self.probe_parameters[id][0].put(t.time()-self.start_times[id])
		
def main():
	probe = Probe()
	probe.InitProbe("first_probe", "Probe 1: Looped %.3f times a second.\n", 5)
	probe.InitProbe("second_probe", "Probe 2: Looped %.3f times a second.\n", 5)
	probe.RunProbes()
	
	while True:
		probe.StartTiming("first_probe")
		for i in range(1000000):
			x = 0
		probe.StopTiming("first_probe")
		
		probe.StartTiming("second_probe")
		for i in range(2000000):
			x = 0
		probe.StopTiming("second_probe")
		
if __name__ == "__main__":
	main()