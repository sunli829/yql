mod group_by;

use anyhow::Result;

use crate::dataset::DataSet;
use crate::expr::physical_expr::PhysicalExpr;
use crate::planner::window::Window;

pub use group_by::{GroupByExprsIter, GroupByWindowIter, GroupedKey};

pub trait DataSetExt {
    fn group_by_exprs(&self, exprs: &mut [PhysicalExpr]) -> Result<GroupByExprsIter>;

    fn group_by_window(&self, time_idx: usize, window: &Window) -> Result<GroupByWindowIter>;
}

impl DataSetExt for DataSet {
    fn group_by_exprs(&self, exprs: &mut [PhysicalExpr]) -> Result<GroupByExprsIter> {
        group_by::group_by_exprs(self, exprs)
    }

    fn group_by_window(&self, time_idx: usize, window: &Window) -> Result<GroupByWindowIter> {
        group_by::group_by_window(self, time_idx, window)
    }
}
