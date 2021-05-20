mod group_by;

use anyhow::Result;
use yql_dataset::DataSet;
use yql_expr::PhysicalExpr;
use yql_planner::Window;

pub use group_by::{GroupByExprsIter, GroupByWindowIter, GroupedKey};

pub trait DataSetExt {
    fn group_by_exprs(&self, exprs: &mut [PhysicalExpr]) -> Result<GroupByExprsIter>;

    fn group_by_window(
        &self,
        time_expr: Option<&mut PhysicalExpr>,
        watermark_expr: Option<&mut PhysicalExpr>,
        current_watermark: &mut Option<i64>,
        window: &Window,
    ) -> Result<GroupByWindowIter>;
}

impl DataSetExt for DataSet {
    fn group_by_exprs(&self, exprs: &mut [PhysicalExpr]) -> Result<GroupByExprsIter> {
        group_by::group_by_exprs(self, exprs)
    }

    fn group_by_window(
        &self,
        time_expr: Option<&mut PhysicalExpr>,
        watermark_expr: Option<&mut PhysicalExpr>,
        current_watermark: &mut Option<i64>,
        window: &Window,
    ) -> Result<GroupByWindowIter> {
        group_by::group_by_window(self, time_expr, watermark_expr, current_watermark, window)
    }
}
