mod aggregate;
mod f_logic;
mod f_ref;
mod math;

use aggregate::*;
use f_logic::*;
use f_ref::*;
use math::*;

use crate::expr::func::Function;

#[rustfmt::skip]
const FUNCS: &[Function] = &[
    // math
    SQRT, SIN, COS, TAN, ASIN, ACOS, ATAN, FLOOR, CEIL, ROUND, TRUNC, ABS, SIGNUM, EXP, LN, LOG2, LOG10,
    
    // aggregate
    AVG, SUM, COUNT, MIN, MAX, FIRST, LAST,
    
    // f.ref
    F_ALL, F_ANY, F_BARSLAST, F_BARSSINCE, F_COUNT, F_DMA, F_EMA, F_FILTER, F_HHV, F_LLV, 
    F_HHVBARS, F_LLVBARS, F_LAST, F_MA, F_MEMA, F_REF, F_SMA, F_SUM, F_WMA,
    
    // f.logic
    F_BETWEEN, F_CROSS, F_LONGCROSS,
];

pub fn find_function(namespace: Option<&str>, name: &str) -> Option<&'static Function> {
    FUNCS.iter().find(|func| match namespace {
        Some(namespace) => match func.namespace {
            Some(func_namespace) => {
                func_namespace.eq_ignore_ascii_case(namespace)
                    && func.name.eq_ignore_ascii_case(&name)
            }
            None => false,
        },
        None => func.namespace.is_none() && func.name.eq_ignore_ascii_case(&name),
    })
}
