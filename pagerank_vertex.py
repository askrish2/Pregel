from vertex import vertex

TOTAL_NODES = 334863

class pagerank_vertex(vertex):

    def __init__(self, vertex_value, vertex_id, outgoing_edges):
        super(pagerank_vertex, self).__init__(vertex_value, vertex_id, outgoing_edges)

    def compute(self, curr_val, messages, outgoing_edges, nodes):
        new_messages = []
        if self.superstep >= 1:
            total = 0
            for (mess_val, val) in messages:
                if "halt" in mess_val:
                    self.active = False
                else:
                    total += val
            self.vertex_value = 0.15/len(nodes) + 0.85 * total
        elif self.superstep < 30:
            for node in outgoing_edges:
                new_messages.append((node, ("val", self.vertex_value/len(outgoing_edges))))
        else:
            new_messages.append((self.vertex_id, ("halt", 0)))
            return new_messages