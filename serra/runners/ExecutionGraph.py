
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
        
        print('Found entry points: ', entry_names)
        return entry_names
    

    def execute(self, block_name: str):

        block = self.name_to_block[block_name]

        inputs = []
        for input_block in block.upstream_blocks:
            # Grab the df from the upstream tasks
            inputs.append(self.outputs[input_block])

        print("Executing block:", block.name)
        print("inputs are:", inputs)

        result = get_result(block, inputs)
        print("Result is", result)
        print()
        self.outputs[block.name] = result



sales = Block("SALES")
ratings = Block("RATINGS")

join = Block("JOIN")
join.add_upstream_task("SALES")
join.add_upstream_task("RATINGS")

write = Block("WRITE")
write.add_upstream_task("JOIN")


graph = BlockGraph([sales, ratings, join, write])

entry_points = graph.find_entry_points()
while len(entry_points) != 0:
    # execute
    graph.execute(entry_points[0])
    entry_points = graph.find_entry_points()