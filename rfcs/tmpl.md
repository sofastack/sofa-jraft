- Feature Name: A snapshot saving would be triggered when the lastAppliedIndex value minus lastSnapshotId value greater than or equal to 'snapshotAppliedIndexMargin'.
- Date: 2022-03-08
- RFC PR: #

## Summary

Provide a configuration which name 'snapshotAppliedIndexMargin' for nodeOptions,
a snapshot saving would be triggered when the lastAppliedIndex value minus lastSnapshotId value is greater than 'snapshotAppliedIndexMargin'.


## Motivation

This feature is necessary, user couldn't save a snapshot at fixed 'appliedIndex' interval without this feature. 
Therefore, I'd like to implement this feature, then user can manage the generation of snapshots by setting property.

## Detailed Design

provide a configuration item which name is 'snapshotAppliedIndexMargin' for nodeOptions, the default value of this property is zero. It only takes effect when 'snapshotAppliedIndexMargin' > 0 && 'snapshotAppliedIndexMargin' <= Integer.MAX_VALUE.

In the "notifyLastAppliedIndexUpdated" method, add a listener. When the index changes, the listener will handle this event. Calculate whether lastAppliedIndex value minus lastSnapshotId value is greater than or equal to 'snapshotAppliedIndexMargin'. if true, trigger snapshot save.

## Drawbacks

Why should we not do this?

## Alternative

- Why is this design the best in the space of possible designs?
- What other designs have been considered and what is the rationale for not
  choosing them?
- What is the impact of not doing this?

## Unresolved questions

What parts of the design are still to be determined?

- When both 'snapshotAppliedIndexMargin' and 'snapshotIntervalSecs' are configured and 'snapshotAppliedIndexMargin' takes effect, is it necessary to make 'snapshotIntervalSecs' invalid?