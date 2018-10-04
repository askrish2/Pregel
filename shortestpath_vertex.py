import sys


SOURCE = 1


class pagerank_vertex(vertex):
    def __init__(self, vertex_value, vertex_id, outgoing_edges):
        self.vertex_value = vertex_value
        self.vertex_id = vertex_id
        self.outgoing_edges = outgoing_edges
        self.active = True
        self.superstep = 1

    def compute(self, curr_val, messages, outgoing_edges, nodes):
        new_messages = []
        mindist = sys.maxint

        if self.vertex_id == SOURCE:
            mindist = 0

        for (mess_val, val) in messages:
            if "halt" in mess_val:
                self.active = False
            else:
                mindist = min(mindist, val)
        if mindist < self.vertex_value:
            self.vertex_value = mindist
            for node in outgoing_edges:
                new_messages.append((node, ("val", mindist + nodes[node].vertex_value)))
        new_messages.append((self.vertex_id, ("halt", 0)))
        self.superstep+=1
        return new_messages