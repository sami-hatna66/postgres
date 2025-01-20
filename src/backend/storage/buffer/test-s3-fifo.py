import sys
import re
import ast
import copy

def process_log_file(log_file):
    s3_logs = []
    with open(log_file, 'r') as f:
        for line in f:
            if "DEBUG S3-FIFO" in line:
                s3_logs.append(line)

    tags = []
    tag_pattern = r"DEBUG S3-FIFO tag (-?\d+)"

    ghost_before = []
    ghost_before_pattern = r"DEBUG S3-FIFO ghost before (\[\s*(\d+(,\s*\d+)*)?\s*\])"
    ghost_after = []
    ghost_after_pattern = r"DEBUG S3-FIFO ghost after (\[\s*(\d+(,\s*\d+)*)?\s*\])"

    main_before = []
    main_before_pattern = r"DEBUG S3-FIFO main before (\[\[.*?\]\,\])"
    main_after = []
    main_after_pattern = r"DEBUG S3-FIFO main after (\[\[.*?\]\,\])"

    probationary_before = []
    probationary_before_pattern = r"DEBUG S3-FIFO probationary before (\[\[.*?\]\,\])"
    probationary_after = []
    probationary_after_pattern = r"DEBUG S3-FIFO probationary after (\[\[.*?\]\,\])"

    for line in s3_logs:
        tag_match = re.search(tag_pattern, line.strip())
        if tag_match:
            tags.append(int(tag_match.group(1)))
            continue

        ghost_before_match = re.search(ghost_before_pattern, line.strip())
        if ghost_before_match:
            ghost_before.append(ast.literal_eval(ghost_before_match.group(1)))
            continue
        
        ghost_after_match = re.search(ghost_after_pattern, line.strip())
        if ghost_after_match:
            ghost_after.append(ast.literal_eval(ghost_after_match.group(1)))
            continue

        main_before_match = re.search(main_before_pattern, line.strip())
        if main_before_match:
            main_before.append(ast.literal_eval(main_before_match.group(1)))
            continue

        main_after_match = re.search(main_after_pattern, line.strip())
        if main_after_match:
            main_after.append(ast.literal_eval(main_after_match.group(1)))
            continue

        probationary_before_match = re.search(probationary_before_pattern, line.strip())
        if probationary_before_match:
            probationary_before.append(ast.literal_eval(probationary_before_match.group(1)))
            continue

        probationary_after_match = re.search(probationary_after_pattern, line.strip())
        if probationary_after_match:
            probationary_after.append(ast.literal_eval(probationary_after_match.group(1)))
            continue

    assert len(ghost_before) == len(ghost_after) == len(main_before) == len(main_after) == len(probationary_before) == len(probationary_after) == len(tags)

    history = []

    for x in range(0, len(ghost_before)):
        history.append({
            "tag": tags[x],
            "ghost_before": ghost_before[x],
            "ghost_after": ghost_after[x],
            "main_before": main_before[x],
            "main_after": main_after[x],
            "probationary_before": probationary_before[x],
            "probationary_after": probationary_after[x]
        })
    
    return history

def fifo_reinsert(queue, value):
    new_value = value
    while True:
        new_value[3] = max(0, new_value[3] - 1)
        queue.append(new_value)
        new_value = queue.pop(0)

        if new_value[2] == 0 and new_value[3] == 0 and new_value[0] != -1:
            break

    return new_value[0], queue

def run_s3_fifo(ptag, pghost, pmain, pprobationary):
    tag = copy.deepcopy(ptag)
    ghost = copy.deepcopy(pghost)
    main = copy.deepcopy(pmain)
    probationary = copy.deepcopy(pprobationary)

    if tag in ghost:
        print("path 1")
        ghost.remove(tag)
        new_idx, main = fifo_reinsert(main, [-1, tag, 0, 0])
        for entry in main:
            if entry[0] == -1:
                entry[0] = new_idx
                break
    else:
        probationary.append([-1, tag, 0, 0])
        evicted = probationary.pop(0)

        # [ idx, tag, ref, usage ]
        if evicted[2] > 0 or evicted[3] > 0:
            new_idx, main = fifo_reinsert(main, [evicted[0], evicted[1], 0, 0])
            for entry in probationary:
                if entry[0] == -1:
                    entry[0] = new_idx
                    break
        else:
            ghost.append(evicted[1])
            # ghost.pop(0)
            for entry in probationary:
                if entry[0] == -1:
                    entry[0] = evicted[0]
                    break
    
    return ghost, main, probationary

def validate_buffer_history(buffer_history):
    discrepancy_count = 0

    for event in buffer_history:
        tag = event["tag"]

        ghost_before = event["ghost_before"]
        ghost_after = event["ghost_after"]

        main_before = event["main_before"]
        main_after = event["main_after"]

        probationary_before = event["probationary_before"]
        probationary_after = event["probationary_after"]

        actual_ghost, actual_main, actual_probationary = run_s3_fifo(tag, ghost_before, main_before, probationary_before)

        output = ""
        if actual_ghost != ghost_after or actual_main != main_after or actual_probationary != probationary_after:
            output += "Discrepancy\n"
            discrepancy_count += 1

        if actual_ghost != ghost_after:
            output += f"ghost before: {ghost_before}\nyour ghost: {ghost_after}\nactual ghost: {actual_ghost}\n"
        if actual_main != main_after:
            output += f"main before: {main_before}\nyour main: {main_after}\nactual main: {actual_main}\n"
        if actual_probationary != probationary_after:
            output += f"probationary before: {probationary_before}\nyour probationary: {probationary_after}\nactual probationary: {actual_probationary}\n"
        
        if output != "": print(output + "\n")
    
    # There may be one or two discrepancies caused by different usage counts
    # These occur because another process will have changed the usage count
    # in the short time interval between relinquishing and reacquiring the 
    # StrategyControl locks after logging "before" and "after"
    # As long as there are only one or two of these discrepancies, there isn't a problem
    print("Validation Complete!")
    print(f"Found {discrepancy_count} {"discrepancy" if discrepancy_count == 1 else "discrepancies"}")


log_file = sys.argv[1]

buffer_history = process_log_file(log_file)

validate_buffer_history(buffer_history)
