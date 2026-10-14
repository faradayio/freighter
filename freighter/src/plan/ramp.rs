//! Cutting a plan's shards into consecutive ramp steps and dealing each
//! step's shards to lanes. Pure.

use std::str::FromStr;

use crate::error::{Error, Result};
use crate::manifest::{ShardRange, ShardSpec};

use super::sizing::{inflight_mutations, LoadPlan};

/// Fractions of the shards per step, in order, summing to 1. One to three
/// of them: the lane ladder has three rungs. Only `FromStr` builds one, so
/// `plan_ramp` never sees an unchecked list.
#[derive(Debug, Clone, PartialEq)]
pub struct Ramp(Vec<f64>);

impl Ramp {
    /// In step order; validated by `FromStr`.
    pub fn fractions(&self) -> &[f64] {
        &self.0
    }
}

impl FromStr for Ramp {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let fractions = s
            .split(',')
            .map(|part| {
                part.trim()
                    .parse::<f64>()
                    .ok()
                    .filter(|f| f.is_finite() && *f > 0.0)
                    .ok_or_else(|| {
                        Error::Refused(format!("ramp fraction {part:?} must be a positive number"))
                    })
            })
            .collect::<Result<Vec<f64>>>()?;
        if !(1..=3).contains(&fractions.len()) {
            return Err(Error::Refused(format!(
                "ramp {s:?}: 1 to 3 steps, got {}",
                fractions.len()
            )));
        }
        let sum: f64 = fractions.iter().sum();
        if (sum - 1.0).abs() > 1e-9 {
            return Err(Error::Refused(format!("ramp fractions {s} must sum to 1")));
        }
        Ok(Ramp(fractions))
    }
}

#[derive(Debug, Clone, PartialEq)]
/// One ramp step: its lanes (the ladder's rung, capped at the step's shard count) and its shards, lane-major.
pub struct RampStep {
    pub index: usize,
    pub lanes: u32,
    /// Lane-major, each lane's ranges ascending — what the mover's lane
    /// rules read.
    pub shards: Vec<ShardSpec>,
    pub inflight_mutations: usize,
}

/// Lanes ramp 1 -> half -> all across the steps, counted from the END of
/// that ladder: a single fraction is one full-rate step, and two fractions
/// run half -> all with NO one-lane step. Every step but the last takes at
/// least one shard.
///
/// Dealing is by CONTIGUOUS BLOCK: lane j takes the j-th equal run of the
/// step's ascending shards, so each lane writes one contiguous key region
/// and the lanes' cursors sit in distant regions. Round-robin would park
/// every cursor in neighbouring tablets, so one tablet server takes every
/// lane's writes at once.
pub fn plan_ramp(plan: &LoadPlan, ramp: &Ramp) -> Result<Vec<RampStep>> {
    let fractions = ramp.fractions();
    let ladder = [1, (plan.lanes / 2).max(1), plan.lanes];
    let step_lanes = &ladder[ladder.len() - fractions.len()..];
    let n = plan.shard_ranges.len();
    let mut steps = Vec::with_capacity(fractions.len());
    let mut cumulative = 0.0;
    let mut start = 0usize;
    for (index, (&fraction, &want_lanes)) in fractions.iter().zip(step_lanes).enumerate() {
        cumulative += fraction;
        // ceil of the ROUNDED product: 0.05 + 0.10 is 0.15000000000000002,
        // and a bare ceil would turn 3.0000000000000004 shards into 4.
        let end = if index == fractions.len() - 1 {
            n
        } else {
            let rounded = (n as f64 * cumulative * 1e9).round() / 1e9;
            (rounded.ceil() as usize).max(start + 1).min(n)
        };
        if end <= start {
            return Err(Error::Refused(format!(
                "ramp step {index} is empty: {n} shards cannot be cut by {fractions:?}"
            )));
        }
        let count = end - start;
        let lanes = want_lanes.min(count as u32);
        let mut shards = Vec::with_capacity(count);
        for lane in 0..lanes {
            let from = lane as usize * count / lanes as usize;
            let to = (lane as usize + 1) * count / lanes as usize;
            for i in from..to {
                let (lo, hi) = plan.shard_ranges[start + i];
                shards.push(ShardSpec {
                    index: (start + i) as u32,
                    range: ShardRange::Int { lo, hi },
                    expect_empty: false,
                    lane,
                });
            }
        }
        steps.push(RampStep {
            index,
            lanes,
            shards,
            inflight_mutations: inflight_mutations(plan.flush_cells, lanes),
        });
        start = end;
    }
    Ok(steps)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::manifest::ShardRange;

    fn plan(shards: usize, lanes: u32) -> LoadPlan {
        LoadPlan {
            shard_ranges: (0..shards as i64).map(|i| (i * 10, i * 10 + 9)).collect(),
            lanes,
            read_streams: 4,
            flush_cells: 8000,
            rung: shards as u32 * 10,
        }
    }

    fn ramp(fractions: &str) -> Ramp {
        fractions.parse().unwrap()
    }

    /// manifest.rs validate_lanes: lanes dense from 0; within a lane, listed
    /// ranges strictly ascending and disjoint.
    fn assert_lane_rules(shards: &[ShardSpec]) {
        crate::manifest::validate_lanes(shards).expect("the mover's lane rules hold");
        for s in shards {
            let ShardRange::Int { lo, hi } = s.range;
            assert!(lo <= hi);
        }
    }

    #[test]
    fn steps_lanes_one_half_all_over_disjoint_ascending_shards() {
        let steps = plan_ramp(&plan(75, 8), &ramp("0.05,0.10,0.85")).unwrap();
        assert_eq!(steps.iter().map(|s| s.lanes).collect::<Vec<_>>(), [1, 4, 8]);
        // ceil(75 x 0.05) = 4 and ceil(75 x 0.15) = 12 shards by the end of
        // steps 0 and 1. One request in flight per lane: 1 -> 4 -> 8 requests.
        assert_eq!(
            steps.iter().map(|s| s.shards.len()).collect::<Vec<_>>(),
            [4, 8, 63]
        );
        assert_eq!(
            steps
                .iter()
                .map(|s| s.inflight_mutations)
                .collect::<Vec<_>>(),
            [8_000, 32_000, 64_000]
        );
        assert_eq!(steps.iter().map(|s| s.index).collect::<Vec<_>>(), [0, 1, 2]);
        for step in &steps {
            assert_lane_rules(&step.shards);
        }
        let mut indices: Vec<u32> = steps
            .iter()
            .flat_map(|s| s.shards.iter().map(|x| x.index))
            .collect();
        indices.sort_unstable();
        assert_eq!(indices, (0..75).collect::<Vec<u32>>());
        let max_of = |i: usize| steps[i].shards.iter().map(|s| s.index).max().unwrap();
        let min_of = |i: usize| steps[i].shards.iter().map(|s| s.index).min().unwrap();
        assert!(max_of(0) < min_of(1));
        assert!(max_of(1) < min_of(2));
    }

    #[test]
    fn deals_contiguous_blocks_lane_major() {
        let steps = plan_ramp(&plan(7, 4), &ramp("1.0")).unwrap();
        assert_eq!(steps.len(), 1);
        let pairs: Vec<(u32, u32)> = steps[0].shards.iter().map(|s| (s.index, s.lane)).collect();
        assert_eq!(
            pairs,
            [(0, 0), (1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (6, 3)]
        );
        assert_eq!(
            steps[0].shards[0],
            ShardSpec {
                index: 0,
                range: ShardRange::Int { lo: 0, hi: 9 },
                expect_empty: false,
                lane: 0
            }
        );
    }

    #[test]
    fn uses_fewer_lanes_than_shards_never_more() {
        let steps = plan_ramp(&plan(20, 16), &ramp("0.05,0.10,0.85")).unwrap();
        assert_eq!(
            steps.iter().map(|s| s.shards.len()).collect::<Vec<_>>(),
            [1, 2, 17]
        );
        assert_eq!(
            steps.iter().map(|s| s.lanes).collect::<Vec<_>>(),
            [1, 2, 16]
        );
        for step in &steps {
            assert_lane_rules(&step.shards);
        }
    }

    #[test]
    fn gives_every_early_step_a_shard_when_there_are_few() {
        let steps = plan_ramp(&plan(3, 2), &ramp("0.05,0.10,0.85")).unwrap();
        assert_eq!(
            steps.iter().map(|s| s.shards.len()).collect::<Vec<_>>(),
            [1, 1, 1]
        );
        let steps = plan_ramp(&plan(10, 8), &ramp("0.05,0.10,0.85")).unwrap();
        assert_eq!(
            steps.iter().map(|s| s.shards.len()).collect::<Vec<_>>(),
            [1, 1, 8]
        );
    }

    #[test]
    fn two_fractions_skip_the_one_lane_step() {
        let steps = plan_ramp(&plan(40, 8), &ramp("0.25,0.75")).unwrap();
        assert_eq!(steps.iter().map(|s| s.lanes).collect::<Vec<_>>(), [4, 8]);
    }

    #[test]
    fn refuses_an_empty_step() {
        let err = plan_ramp(&plan(2, 2), &ramp("0.05,0.10,0.85")).unwrap_err();
        assert!(err.to_string().contains("ramp step 2 is empty"), "{err}");
    }

    #[test]
    fn refuses_when_there_are_fewer_shards_than_steps() {
        let err = plan_ramp(&plan(1, 2), &ramp("0.05,0.10,0.85")).unwrap_err();
        assert!(err.to_string().contains("ramp step 1 is empty"), "{err}");
    }

    #[test]
    fn ramp_parses_fractions_and_refuses_bad_ones() {
        assert_eq!("1.0".parse::<Ramp>().unwrap(), Ramp(vec![1.0]));
        assert_eq!(
            "0.05,0.10,0.85".parse::<Ramp>().unwrap(),
            Ramp(vec![0.05, 0.10, 0.85])
        );
        let err = "0.5,0.4".parse::<Ramp>().unwrap_err();
        assert!(err.to_string().contains("must sum to 1"), "{err}");
        let err = "0.25,0.25,0.25,0.25".parse::<Ramp>().unwrap_err();
        assert!(err.to_string().contains("1 to 3 steps"), "{err}");
        assert!("".parse::<Ramp>().is_err());
        assert!("a,b".parse::<Ramp>().is_err());
        assert!("nan".parse::<Ramp>().is_err());
        assert!("inf".parse::<Ramp>().is_err());
        assert!("1.5,-0.5".parse::<Ramp>().is_err());
        let err = "0,1".parse::<Ramp>().unwrap_err();
        assert!(err.to_string().contains("positive"), "{err}");
    }
}
