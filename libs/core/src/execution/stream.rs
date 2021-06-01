use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use futures_util::stream::{Stream, StreamExt};

use crate::dataset::{DataSet, SchemaRef};
use crate::execution::execution_context::ExecutionContext;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::physical_plan::PhysicalPlan;
use crate::ExecutionMetrics;

pub struct CreateStreamContext {
    pub ctx: Arc<ExecutionContext>,
    pub prev_state: HashMap<usize, Vec<u8>>,
}

pub struct DataSetWithWatermark {
    pub watermark: Option<i64>,
    pub dataset: DataSet,
}

pub trait DataSetStream: Stream<Item = Result<DataSetWithWatermark>> {
    fn save_state(&self, state: &mut HashMap<usize, Vec<u8>>) -> Result<()>;
}

pub type BoxDataSetStream = Pin<Box<dyn DataSetStream + Send + 'static>>;

pub struct DataStream {
    ctx: Arc<ExecutionContext>,
    input: BoxDataSetStream,
    schema: SchemaRef,
}

impl DataStream {
    pub(crate) fn new(plan: LogicalPlan, state: Option<Vec<u8>>) -> Result<Self> {
        let physical_plan = PhysicalPlan::try_new(plan)?;

        // load previous state
        let prev_state: HashMap<usize, Vec<u8>> = match state {
            Some(data) => {
                bincode::deserialize(&data).context("failed to deserialize stream state.")?
            }
            None => Default::default(),
        };

        let exec_ctx = Arc::new(ExecutionContext::new());
        let mut create_ctx = CreateStreamContext {
            ctx: exec_ctx.clone(),
            prev_state,
        };

        Ok(Self {
            ctx: exec_ctx,
            schema: physical_plan.root.schema(),
            input: crate::execution::streams::create_stream(&mut create_ctx, physical_plan.root)?,
        })
    }

    pub fn save_state(&self) -> Result<Vec<u8>> {
        let mut state = Default::default();
        self.input.save_state(&mut state)?;
        Ok(bincode::serialize(&state)?)
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn metrics(&self) -> ExecutionMetrics {
        self.ctx.metrics()
    }
}

impl Stream for DataStream {
    type Item = Result<DataSet>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(dataset))) => Poll::Ready(Some(Ok(dataset.dataset))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::StreamExt;

    use crate::array::DataType;
    use crate::dataset::{CsvOptions, DataSet, Field, Schema};
    use crate::dsl::*;
    use crate::sources::csv::{Options, Provider};
    use crate::{DataFrame, SourceProviderWrapper, Window};

    fn create_source_provider() -> Provider {
        let schema = Arc::new(
            Schema::try_new(vec![
                Field::new("time", DataType::Timestamp(None)),
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::String),
                Field::new("c", DataType::String),
            ])
            .unwrap(),
        );

        let data = r#"
1622512140000,1,a,a
1622512200000,2,b,a
1622512260000,3,c,a
1622512320000,4,d,a
1622512380000,5,e,b
1622512440000,6,f,b
1622512500000,7,g,b
1622512560000,8,h,b
1622512620000,9,i,b
1622512680000,10,j,b
1622512740000,11,k,b
1622512800000,12,l,b
1622512860000,13,m,c
1622512920000,14,n,c
1622512980000,15,o,c
1622513040000,16,p,c
1622513100000,17,q,c
1622513160000,18,r,c
1622513220000,19,s,c
1622513280000,20,t,c
1622513340000,21,u,d
1622513400000,22,v,d
1622513460000,23,w,d
1622513520000,24,x,d
1622513580000,25,y,d
1622513640000,26,z,d
"#;

        Provider::new_from_memory(
            Options {
                delimiter: b',',
                has_header: false,
                batch_size: 10,
            },
            schema,
            data,
        )
    }

    #[tokio::test]
    async fn test_source_stream() {
        let provider = create_source_provider();
        let df = DataFrame::new(
            Arc::new(SourceProviderWrapper(provider)),
            None,
            Some(col("time")),
            None,
        );
        let output_schema = Arc::new(
            Schema::try_new(vec![
                Field::new("time", DataType::Timestamp(None)),
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::String),
                Field::new("c", DataType::String),
                Field::new("@time", DataType::Timestamp(None)),
            ])
            .unwrap(),
        );

        let mut stream = df.clone().into_stream(None).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema.clone(),
                CsvOptions::default(),
                br#"
1622512140000,1,a,a,1622512140000
1622512200000,2,b,a,1622512200000
1622512260000,3,c,a,1622512260000
1622512320000,4,d,a,1622512320000
1622512380000,5,e,b,1622512380000
1622512440000,6,f,b,1622512440000
1622512500000,7,g,b,1622512500000
1622512560000,8,h,b,1622512560000
1622512620000,9,i,b,1622512620000
1622512680000,10,j,b,1622512680000
"#,
            )
            .unwrap()
        );

        let state = stream.save_state().unwrap();
        let mut stream = df.clone().into_stream(Some(state)).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema.clone(),
                CsvOptions::default(),
                br#"
1622512740000,11,k,b,1622512740000
1622512800000,12,l,b,1622512800000
1622512860000,13,m,c,1622512860000
1622512920000,14,n,c,1622512920000
1622512980000,15,o,c,1622512980000
1622513040000,16,p,c,1622513040000
1622513100000,17,q,c,1622513100000
1622513160000,18,r,c,1622513160000
1622513220000,19,s,c,1622513220000
1622513280000,20,t,c,1622513280000
"#,
            )
            .unwrap()
        );

        let state = stream.save_state().unwrap();
        let mut stream = df.clone().into_stream(Some(state)).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema,
                CsvOptions::default(),
                br#"
1622513340000,21,u,d,1622513340000
1622513400000,22,v,d,1622513400000
1622513460000,23,w,d,1622513460000
1622513520000,24,x,d,1622513520000
1622513580000,25,y,d,1622513580000
1622513640000,26,z,d,1622513640000
"#,
            )
            .unwrap()
        );

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_projection_stream() {
        let provider = create_source_provider();
        let df = DataFrame::new(
            Arc::new(SourceProviderWrapper(provider)),
            None,
            Some(col("time")),
            None,
        )
        .select(vec![(col("a") + value(88)).alias("a")]);
        let output_schema =
            Arc::new(Schema::try_new(vec![Field::new("a", DataType::Int64)]).unwrap());

        let mut stream = df.clone().into_stream(None).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema.clone(),
                CsvOptions::default(),
                br#"
89,
90,
91,
92,
93,
94,
95,
96,
97,
98,
"#,
            )
            .unwrap()
        );

        let state = stream.save_state().unwrap();
        let mut stream = df.clone().into_stream(Some(state)).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema.clone(),
                CsvOptions::default(),
                br#"
99,
100,
101,
102,
103,
104,
105,
106,
107,
108,
"#,
            )
            .unwrap()
        );

        let state = stream.save_state().unwrap();
        let mut stream = df.clone().into_stream(Some(state)).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema,
                CsvOptions::default(),
                br#"
109,
110,
111,
112,
113,
114,
"#,
            )
            .unwrap()
        );

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_filter_stream() {
        let provider = create_source_provider();
        let df = DataFrame::new(
            Arc::new(SourceProviderWrapper(provider)),
            None,
            Some(col("time")),
            None,
        )
        .filter((col("a") % value(2)).eq(value(0)));
        let output_schema = Arc::new(
            Schema::try_new(vec![
                Field::new("time", DataType::Timestamp(None)),
                Field::new("a", DataType::Int64),
                Field::new("b", DataType::String),
                Field::new("c", DataType::String),
                Field::new("@time", DataType::Timestamp(None)),
            ])
            .unwrap(),
        );

        let mut stream = df.clone().into_stream(None).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema.clone(),
                CsvOptions::default(),
                br#"
1622512200000,2,b,a,1622512200000
1622512320000,4,d,a,1622512320000
1622512440000,6,f,b,1622512440000
1622512560000,8,h,b,1622512560000
1622512680000,10,j,b,1622512680000
"#,
            )
            .unwrap()
        );

        let state = stream.save_state().unwrap();
        let mut stream = df.clone().into_stream(Some(state)).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema.clone(),
                CsvOptions::default(),
                br#"
1622512800000,12,l,b,1622512800000
1622512920000,14,n,c,1622512920000
1622513040000,16,p,c,1622513040000
1622513160000,18,r,c,1622513160000
1622513280000,20,t,c,1622513280000
"#,
            )
            .unwrap()
        );

        let state = stream.save_state().unwrap();
        let mut stream = df.clone().into_stream(Some(state)).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema,
                CsvOptions::default(),
                br#"
1622513400000,22,v,d,1622513400000
1622513520000,24,x,d,1622513520000
1622513640000,26,z,d,1622513640000
"#,
            )
            .unwrap()
        );

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_aggregate_stream() {
        let provider = create_source_provider();
        let df = DataFrame::new(
            Arc::new(SourceProviderWrapper(provider)),
            None,
            Some(col("time")),
            None,
        )
        .aggregate(
            vec![col("c")],
            vec![col("c"), call("sum", vec![col("a")]).alias("a")],
            Window::Fixed {
                length: 1000 * 60 * 60,
            },
        );
        let output_schema = Arc::new(
            Schema::try_new(vec![
                Field::new("c", DataType::String),
                Field::new("a", DataType::Float64),
                Field::new("@time", DataType::Timestamp(None)),
            ])
            .unwrap(),
        );

        let mut stream = df.clone().into_stream(None).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema.clone(),
                CsvOptions::default(),
                br#"
a,10,1622509200000
b,56,1622509200000
"#,
            )
            .unwrap()
        );

        let state = stream.save_state().unwrap();
        let mut stream = df.clone().into_stream(Some(state)).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            DataSet::from_csv_slice(
                output_schema.clone(),
                CsvOptions::default(),
                br#"
b,12,1622512800000
c,132,1622512800000
d,141,1622512800000
"#,
            )
            .unwrap()
        );

        assert!(stream.next().await.is_none());
    }
}
