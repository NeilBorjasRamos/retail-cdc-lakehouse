"""Deterministic revision history, independent from storage and arrival order."""

from collections import defaultdict

from retail.contract import MAX_RECORDS, ContractError, digest


def materialize(events):
    if len(events) > MAX_RECORDS:
        raise ContractError("Reference engine retains at most 100000 transport records")
    versions = defaultdict(dict)
    routing = {}
    for event in events:
        route = (event["topic"], event["partition"])
        key = event["key"]
        if key in routing and routing[key] != route:
            raise ContractError("A key changed topic/partition; explicit migration required")
        routing[key] = route
        state = event["state"]
        if state is None:
            continue
        # Adding nullable region is backward compatible, including semantic replay.
        state = {**state, "region": state.get("region")}
        revision = state["revision"]
        previous = versions[key].get(revision)
        if previous and digest(previous) != digest(state):
            raise ContractError("Same order/revision has conflicting content")
        versions[key][revision] = state
    current, history = [], []
    for key in sorted(versions):
        ordered = sorted(versions[key].values(), key=lambda x: x["revision"])
        if any(state["deleted"] for state in ordered[:-1]):
            raise ContractError("Order ID reuse after deletion is unsupported")
        for index, state in enumerate(ordered):
            end = ordered[index + 1]["revision"] if index + 1 < len(ordered) else None
            history.append({**state, "valid_to_revision": end})
        if not ordered[-1]["deleted"]:
            current.append(ordered[-1])
    aggregates = defaultdict(lambda: {"orders": 0, "amount_minor": 0})
    for state in current:
        key = (state["updated_at"][:10], state["currency"], state["status"])
        aggregates[key]["orders"] += 1
        aggregates[key]["amount_minor"] += state["amount_minor"]
    gold = [
        {"date": key[0], "currency": key[1], "status": key[2], **value}
        for key, value in sorted(aggregates.items())
    ]
    changes = [
        {k: event[k] for k in ("topic", "partition", "offset", "raw_hash", "raw")}
        for event in events
    ]
    return {"changes": changes, "current": current, "history": history, "gold": gold}
