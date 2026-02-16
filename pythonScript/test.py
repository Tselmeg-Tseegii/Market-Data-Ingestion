import sys

print("hi python", flush = True)
while True:
    try:
        line = input()
        print(f"Read: {line}")
    except EOFError:
        # Stop the loop when input ends
        break
