from libp2p.pubsub.score import ScoreParams, TopicScoreParams


def custom_score_params() -> ScoreParams:
    return ScoreParams(
        # P1: Reward time in mesh (longevity = trust)
        p1_time_in_mesh=TopicScoreParams(
            weight=1.0,  # Positive weight rewards staying in mesh
            cap=100.0,  # Max score contribution (e.g., after 100 heartbeat intervals)
            decay=0.99,  # Slow decay to reward long-term peers
        ),
        # P2: First message deliveries (low reward since key never changes)
        p2_first_message_deliveries=TopicScoreParams(
            weight=0.1,  # Small reward
            cap=5.0,  # Very low cap
            decay=0.9,
        ),
        # P3 is default
        p3_mesh_message_deliveries=TopicScoreParams(
            weight=0.5,
            cap=20.0,
            decay=0.95,
        ),
        # P4: Invalid messages (heavy penalty)
        p4_invalid_messages=TopicScoreParams(
            weight=50.0,  # Heavy penalty per invalid message
            cap=500.0,  # Max penalty
            decay=0.7,  # Slower decay = longer punishment
        ),
        # P5: Behavior penalty (for duplicate heartbeats)
        p5_behavior_penalty_weight=10.0,  # Each violation hurts score by 10
        p5_behavior_penalty_decay=0.5,  # Decays over time (forgiveness)
        p5_behavior_penalty_threshold=1.0,  # Penalty kicks in after 1 violation
        # Thresholds
        publish_threshold=0.0,  # Score >= 0 to publish
        graylist_threshold=-10.0,  # 3 violations = graylisted
    )
