
class Block:
    def __init__(self, name):
        self.name = name
        self.downstream_blocks = []
        self.upstream_blocks = []

    def add_downstream_task(self, downstream_task_name):
        self.downstream_blocks.append(downstream_task_name)

    def add_upstream_task(self, upstream_task_name):
        self.upstream_blocks.append(upstream_task_name)

    def has_no_dependencies(self):
        return len(self.upstream_blocks) == 0

def get_result(block: Block, inputs):
    # Just for playing around
    result = f"[{block.name}"
    for input in inputs:
        result = result + "|" + input
    result = result + "]"
    return result

class BlockGraph:
    def __init__(self, blocks):
        self.blocks: list[Block] = blocks
        self.name_to_block = {}
        for block in blocks:
            self.name_to_block[block.name] = block

        self.outputs = {}

    def add_block(self, block_name, deps):
        block = Block(block_name)
        for dep in deps:
            block.add_upstream_task(dep)

        self.blocks.append(block)
        self.name_to_block[block_name] = block

    def find_entry_points(self):
        entry_names = []

        # Get blocks that have not been executed yet
        possible_blocks = []
        for block in self.blocks:
            if block.name not in self.outputs:
                possible_blocks.append(block)

        for block in possible_blocks:
            ok_to_run = True

            for dep in block.upstream_blocks:
                if dep not in self.outputs:
                    # We don't have the available inputs yet
                    ok_to_run = False
                    break
            
            if ok_to_run:
                entry_names.append(block.name)
        
        return entry_names
    

    def execute(self, block_name: str):

        block = self.name_to_block[block_name]

        inputs = []
        for input_block in block.upstream_blocks:
            # Grab the df from the upstream tasks
            inputs.append(self.outputs[input_block])

        result = get_result(block, inputs)
        self.outputs[block.name] = result

if __name__=="__main__":
    # Example of usage
    graph = BlockGraph([])
    graph.add_block("SALES", [])
    graph.add_block("RATINGS", [])
    graph.add_block("JOIN", ["SALES", "RATINGS"])
    graph.add_block("WRITE", ["JOIN"])

    order = []
    entry_points = graph.find_entry_points()
    while len(entry_points) != 0:
        # execute
        order.append(entry_points[0])
        graph.execute(entry_points[0])
        entry_points = graph.find_entry_points()

    print(order)