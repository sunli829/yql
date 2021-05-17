mod aggregate;
mod math;

use aggregate::*;
use math::*;

use crate::func::Function;

#[rustfmt::skip]
pub const FUNCS: &[Function] = &[
    // math
    SQRT, SIN, COS, TAN, ASIN, ACOS, ATAN, FLOOR, CEIL, ROUND, TRUNC, ABS, SIGNUM, EXP, LN, LOG2, LOG10,
    
    // aggregate
    AVG, SUM, COUNT, MIN, MAX, FIRST, LAST,
];
