import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List
from typing import Dict
from typing import Tuple


class AccountValue:
    def __init__(self, name: str, balance: int):
        self.name = name
        self.balance = balance


class AccountUpdate:
    def __init__(self, name: str, balance_change: int):
        self.name = name
        self.balance_change = balance_change


class AccountState:
    def __init__(self, accounts: Dict[str, int]):
        self.accounts = accounts
        self._lock = threading.Lock()

    def get_account(self, name: str) -> AccountValue:
        with self._lock:
            balance = self.accounts.get(name, 0)
            return AccountValue(name, balance)

    def execute_updates(self, updates: List[AccountUpdate]):
        with self._lock:
            # Validate the updates
            for update in updates:
                current_balance = self.accounts.get(update.name, 0)
                if current_balance + update.balance_change < 0:
                    return

            for update in updates:
                if update.name in self.accounts:
                    self.accounts[update.name] += update.balance_change
                else:
                    self.accounts[update.name] = max(0, update.balance_change)


class Transaction:
    def updates(self, state: AccountState) -> Tuple[List[AccountUpdate], bool]:
        raise NotImplementedError()


class Transfer(Transaction):
    def __init__(self, from_acc: str, to_acc: str, value: int):
        self.from_acc = from_acc
        self.to_acc = to_acc
        self.value = value

    def updates(self, state: AccountState) -> Tuple[List[AccountUpdate], bool]:
        from_account = state.get_account(self.from_acc)
        if from_account.balance < self.value:
            return [], False
        return [
            AccountUpdate(self.from_acc, -self.value),
            AccountUpdate(self.to_acc, self.value)
        ], True


class Block:
    def __init__(self, transactions: List[Transaction]):
        self.transactions = transactions


def execute_block(block: Block, values: List[AccountValue]) -> List[AccountValue]:
    results = []

    # Construct the AccountState based on the AccountValues, as the inputs are AccountValue list
    account_state = AccountState({account_value.name: account_value.balance for account_value in values})

    def process_transaction(transaction: Transaction):
        updates, success = transaction.updates(account_state)
        if success:
            account_state.execute_updates(updates)

    # We can scale works with it, e.g ThreadPoolExecutor(max_workers=50)
    with ThreadPoolExecutor() as executor:
        executor.map(process_transaction, block.transactions)

    return [AccountValue(name, balance) for name, balance in account_state.accounts.items()]


# First test case
account_values_example_1 = [
    AccountValue("A", 20),
    AccountValue("B", 30),
    AccountValue("C", 40),
]
block_example_1 = Block([
    Transfer("A", "B", 5),
    Transfer("B", "C", 10),
    Transfer("B", "C", 30)
])
expected_output_1 = [
    AccountValue("A", 15),
    AccountValue("B", 25),
    AccountValue("C", 50),
]

# Another test case
account_values_example_2 = [
    AccountValue("A", 10),
    AccountValue("B", 20),
    AccountValue("C", 30),
    AccountValue("D", 40),
]
block_example_2 = Block([
    Transfer("A", "B", 5),
    Transfer("C", "D", 10),
])
expected_output_2 = [
    AccountValue("A", 5),
    AccountValue("B", 25),
    AccountValue("C", 20),
    AccountValue("D", 50),
]

def test_case(blocks, account_values, expected_output):
    actual_results = execute_block(blocks, account_values)
    for index, account_value in enumerate(actual_results):
        # print(account_value.name, account_value.balance)
        assert account_value.name == expected_output[index].name
        assert account_value.balance == expected_output[index].balance

# Running multiple times to ensure deterministic
for _ in range(10):
    test_case(block_example_1, account_values_example_1, expected_output_1)

for _ in range(10):
    test_case(block_example_2, account_values_example_2, expected_output_2)
