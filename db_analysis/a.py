import random
import re

def check_hostname(hostname: str):
    pattern = r"Server\[\d+\]"
    print(re.findall(pattern, hostname))
    if re.findall(pattern, hostname):
        num = random.randint(10000, 99999)
        return f"Server{num}"
    return hostname

if __name__ == "__main__":
    hostname = ["Server7", "Server[8]"]
    for name in hostname:
        print(check_hostname(name))