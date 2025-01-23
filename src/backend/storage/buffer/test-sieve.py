import sys
import re
import ast

def process_log_file(log_file):
    history_before = []
    history_after = []
    history_hand = []

    before_pattern = r"DEBUG SIEVE before (\[\[.*?\]\,\])"
    after_pattern = r"DEBUG SIEVE after (\[\[.*?\]\,\])"
    hand_pattern = r"DEBUG SIEVE hand position (\d+)"

    with open(log_file, 'r') as f:
        for line in f:
            before_match = re.search(before_pattern, line.strip())
            if before_match:
                history_before.append(ast.literal_eval(before_match.group(1)))
                continue

            after_match = re.search(after_pattern, line.strip())
            if after_match:
                history_after.append(ast.literal_eval(after_match.group(1)))
                continue

            hand_match = re.search(hand_pattern, line.strip())
            if hand_match:
                history_hand.append(ast.literal_eval(hand_match.group(1)))
                continue
    
    assert len(history_hand) == len(history_after) == len(history_before)

    history = []

    for x in range(0, len(history_hand)):
        history.append({
            "hand": history_hand[x],
            "before": history_before[x],
            "after": history_after[x]
        })
    
    return history

def run_sieve(before, hand):
    buffer = before

    while True:
        # [ buffer desc index, reference count, usage count ]
        idx = buffer[hand][0]
        ref = buffer[hand][1]
        usage = buffer[hand][2]

        if ref > 0 or usage > 0:
            buffer[hand][2] = 0
            hand = hand - 1 if hand - 1 >= 0 else len(buffer) - 1
        else:
            buffer.insert(0, buffer.pop(buffer.index(buffer[hand])))
            break

    return buffer

def validate_buffer_history(buffer_history):
    discrepancy_count = 0

    for event in buffer_history:
        before = event["before"]
        after = event["after"]
        hand = event["hand"]

        actual_after = run_sieve(before, hand)

        if after != actual_after:
            print(f"Discrepancy\nhand: {hand}\nbefore: {before}\nyours: {after}\nactual: {actual_after}\n\n")
            discrepancy_count += 1

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
