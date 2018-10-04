import abc

class vertex(abc.ABCMeta):

	def __init__(self, vertex_value, vertex_id, outgoing_edges):
		self.vertex_value = vertex_value
		self.vertex_id = vertex_id
		self.outgoing_edges = outgoing_edges
		self.active = True
		self.superstep = 0

	@abc.abstractmethod
        def compute(self, curr_val, messages, outgoing_edges, nodes):
            pass

