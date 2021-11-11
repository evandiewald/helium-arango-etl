import enum


class TransactionType(enum.Enum):
    coinbase_v1 = 1
    security_coinbase_v1 = 2
    oui_v1 = 3
    gen_gateway_v1 = 4
    routing_v1 = 5
    payment_v1 = 6
    security_exchange_v1 = 7
    consensus_group_v1 = 8
    add_gateway_v1 = 9
    assert_location_v1 = 10
    create_htlc_v1 = 11
    redeem_htlc_v1 = 12
    poc_request_v1 = 13
    poc_receipts_v1 = 14
    vars_v1 = 14
    rewards_v1 = 16
    token_burn_v1 = 17
    dc_coinbase_v1 = 18
    token_burn_exchange_rate_v1 = 19
    payment_v2 = 20
    state_channel_open_v1 = 21
    state_channel_close_v1 = 22
    price_oracle_v1 = 23
    transfer_hotspot_v1 = 24
    rewards_v2 = 25
    assert_location_v2 = 26
    gen_validator_v1 = 27
    stake_validator_v1 = 28
    unstake_validator_v1 = 29
    validator_heartbeat_v1 = 30
    transfer_validator_stake_v1 = 31
    gen_price_oracle_v1 = 32
    consensus_group_failure_v1 = 33
    transfer_hotspot_v2 = 34


class GatewayMode(enum.Enum):
    full = 1
    light = 2
    dataonly = 3


class TransactionActorRole(enum.Enum):
    payee = 1
    payer = 2
    owner = 3
    gateway = 4
    reward_gateway = 5
    challenger = 6
    challengee = 7
    witness = 8
    consensus_member = 9
    escrow = 10
    sc_opener = 11
    sc_closer = 12
    packet_receiver = 13
    oracle = 14
    router = 15
    validator = 16
    consensus_failure_member = 17
    consensus_failure_failed_member = 18
