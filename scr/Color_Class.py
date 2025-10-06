# --- definizione colori ANSI ---
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[1;32m"
YELLOW = "\033[1;33m"
RED = "\033[1;31m"
CYAN = "\033[1;36m"

def print_info(msg):
    print(f"{GREEN}[INFO]{RESET} {msg}")

def print_warn(msg):
    print(f"{YELLOW}[WARNING]{RESET} ⚠ {msg}")

def print_error(msg):
    print(f"{RED}[ERROR]{RESET} ❌ {msg}")

def print_step(msg):
    print(f"{CYAN}>>> {msg}{RESET}")
