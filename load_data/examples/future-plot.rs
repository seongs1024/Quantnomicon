use tracing::{ info, error };

#[path = "./future-trend.rs"] mod trend;
use trend::tr::trend;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let files = vec![
        // "2022-04.csv",
        // "2022-05.csv",
        "2022-06.csv",
    ];
    let sigma_for_abnormal = 2.0f64;
    let df = trend(files, sigma_for_abnormal).unwrap();
    // plot::plot_correlations(&df);
    plot::plot_trend(&df);
}

pub mod plot {
    use polars::prelude::*;
    use plotters::prelude::*;
    const OUT_FILE_NAME: &'static str = "./scatters.svg";

    pub fn plot_trend(data: &DataFrame) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let primaries = data.column("primary").unwrap().f64().unwrap();
        let secondaries = data.column("secondary").unwrap().f64().unwrap();
        let trends = data.column("trend").unwrap().u32().unwrap();

        let area = SVGBackend::new(OUT_FILE_NAME, (1024, 760)).into_drawing_area();

        area.fill(&WHITE)?;

        let x_axis = (primaries.min().unwrap()..primaries.max().unwrap()).step(0.001);
        let y_axis = (secondaries.min().unwrap()..secondaries.max().unwrap()).step(0.001);

        let mut chart = ChartBuilder::on(&area)
            .set_label_area_size(LabelAreaPosition::Left, 40u32)
            .set_label_area_size(LabelAreaPosition::Bottom, 40u32)
            .margin(15u32)
            .caption(format!("Primary-Secondary"), ("sans", 20u32))
            .build_cartesian_2d(x_axis.clone(), y_axis.clone())?;

        chart
            .configure_mesh()
            .x_desc("Primary")
            // .y_desc("Rising and Falling")
            .axis_desc_style(("sans-serif", 15u32))
            .draw()?;

        chart
            .draw_series(
                (3..data.height()).into_iter().map(|row| {
                    TriangleMarker::new(
                        (primaries.get(row).unwrap(), secondaries.get(row).unwrap()),
                        5i32,
                        match trends.get(row) {
                            Some(2u32) => BLUE.mix(0.5).filled(),
                            Some(1u32) => RED.mix(0.5).filled(),
                            Some(3u32) => GREEN.mix(0.5).filled(),
                            _ => YELLOW.mix(0.5).filled(),
                        }
                    )
                }),
            )?;

        // To avoid the IO failure being ignored silently, we manually call the present function
        area.present().expect("Unable to write result to file, please make sure 'plotters-doc-data' dir exists under current dir");
        println!("Result has been saved to {}", OUT_FILE_NAME);
        Ok(())
    }

    pub fn plot_correlations(data: &DataFrame) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let primaries = data.column("primary").unwrap().f64().unwrap();
        let secondaries = data.column("secondary").unwrap().f64().unwrap();
        let risings = data.column("rising").unwrap().f64().unwrap();
        let fallings = data.column("falling").unwrap().f64().unwrap();
        let omh = data.column("offset to max high").unwrap().u32().unwrap();
        let oml = data.column("offset to min low").unwrap().u32().unwrap();

        let area = SVGBackend::new(OUT_FILE_NAME, (1024, 760)).into_drawing_area();

        area.fill(&WHITE)?;

        let x_axis = (primaries.min().unwrap()..primaries.max().unwrap()).step(0.001);
        let y_axis = (secondaries.min().unwrap()..secondaries.max().unwrap()).step(0.001);
        let z_axis = (fallings.min().unwrap()..risings.max().unwrap()).step(0.001);

        let (top, bottom) = area.split_vertically(380u32);
        let (area_3d, area_yz) = top.split_horizontally(512u32);
        let (area_xy, area_xz) = bottom.split_horizontally(512u32);

        ///
        /// 3d
        /// 
        let mut chart_3d = ChartBuilder::on(&area_3d)
            .caption(format!("3D Plot"), ("sans", 20i32))
            .build_cartesian_3d(x_axis.clone(), y_axis.clone(), z_axis.clone())?;
        
        chart_3d.with_projection(|mut pb| {
            pb.yaw = 0.5;
            pb.scale = 0.9;
            pb.into_matrix()
        });

        chart_3d
            .configure_axes()
            .light_grid_style(BLACK.mix(0.15))
            //.max_light_lines(3i32)
            .draw()?;

        chart_3d
            .draw_series(
                (3..data.height()).into_iter().map(|row| {
                    TriangleMarker::new(
                        (primaries.get(row).unwrap(), secondaries.get(row).unwrap(), risings.get(row).unwrap()),
                        5i32,
                        BLUE.mix(0.5).filled()
                    )
                }),
            )?
            .label("BULL")
            .legend(|(x, y)| TriangleMarker::new((x + 5, y), 5u32, BLUE.mix(0.5).filled()));
        
        chart_3d
            .draw_series(
                (3..data.height()).into_iter().map(|row| {
                    Cross::new(
                        (primaries.get(row).unwrap(), secondaries.get(row).unwrap(), fallings.get(row).unwrap()),
                        5i32,
                        RED.mix(0.5)
                    )
                }),
            )?
            .label("BEAR")
            .legend(|(x, y)| Cross::new((x + 5, y), 5u32, RED.mix(0.5)));

        chart_3d
            .configure_series_labels()
            .border_style(&BLACK)
            .draw()?;

        ///
        /// yz
        /// 
        let mut chart_yz = ChartBuilder::on(&area_yz)
            .set_label_area_size(LabelAreaPosition::Left, 40u32)
            .set_label_area_size(LabelAreaPosition::Bottom, 40u32)
            .margin(15u32)
            .caption(format!("Secondary-R.F."), ("sans", 20i32))
            .build_cartesian_2d(y_axis.clone(), z_axis.clone())?;

        chart_yz
            .configure_mesh()
            .x_desc("Secondary")
            // .y_desc("Rising and Falling")
            .axis_desc_style(("sans-serif", 15u32))
            .draw()?;

        chart_yz
            .draw_series(
                (3..data.height()).into_iter().map(|row| {
                    TriangleMarker::new(
                        (secondaries.get(row).unwrap(), risings.get(row).unwrap()),
                        5i32,
                        BLUE.mix(0.5).filled()
                    )
                }),
            )?;
        
        chart_yz
            .draw_series(
                (3..data.height()).into_iter().map(|row| {
                    Cross::new(
                        (secondaries.get(row).unwrap(), fallings.get(row).unwrap()),
                        5i32,
                        RED.mix(0.5)
                    )
                }),
            )?;

        ///
        /// xy
        /// 
        let mut chart_xy = ChartBuilder::on(&area_xy)
            .set_label_area_size(LabelAreaPosition::Left, 40u32)
            .set_label_area_size(LabelAreaPosition::Bottom, 40u32)
            .margin(15u32)
            .caption(format!("Primary-Secondary"), ("sans", 20u32))
            .build_cartesian_2d(x_axis.clone(), y_axis.clone())?;

        chart_xy
            .configure_mesh()
            .x_desc("Primary")
            // .y_desc("Rising and Falling")
            .axis_desc_style(("sans-serif", 15u32))
            .draw()?;

        chart_xy
            .draw_series(
                (3..data.height()).into_iter().map(|row| {
                    TriangleMarker::new(
                        (primaries.get(row).unwrap(), secondaries.get(row).unwrap()),
                        5i32,
                        match (omh.get(row).unwrap(), oml.get(row).unwrap()) {
                            (bull, bear) if bull < bear => BLUE.mix(0.5).filled(),
                            (bull, bear) if bull > bear => RED.mix(0.5).filled(),
                            _ => GREEN.mix(0.5).filled(),
                        }
                    )
                }),
            )?;

        ///
        /// xz
        /// 
        let mut chart_xz = ChartBuilder::on(&area_xz)
            .set_label_area_size(LabelAreaPosition::Left, 40)
            .set_label_area_size(LabelAreaPosition::Bottom, 40)
            .margin(15)
            .caption(format!("Primary-R.F."), ("sans", 20i32))
            .build_cartesian_2d(x_axis.clone(), z_axis.clone())?;

        chart_xz
            .configure_mesh()
            .x_desc("Primary")
            // .y_desc("Rising and Falling")
            .axis_desc_style(("sans-serif", 15))
            .draw()?;

        chart_xz
            .draw_series(
                (3..data.height()).into_iter().map(|row| {
                    TriangleMarker::new(
                        (primaries.get(row).unwrap(), risings.get(row).unwrap()),
                        5i32,
                        BLUE.mix(0.5).filled()
                    )
                }),
            )?;
        
        chart_xz
            .draw_series(
                (3..data.height()).into_iter().map(|row| {
                    Cross::new(
                        (primaries.get(row).unwrap(), fallings.get(row).unwrap()),
                        5i32,
                        RED.mix(0.5)
                    )
                }),
            )?;

        // To avoid the IO failure being ignored silently, we manually call the present function
        area.present().expect("Unable to write result to file, please make sure 'plotters-doc-data' dir exists under current dir");
        println!("Result has been saved to {}", OUT_FILE_NAME);
        Ok(())
    }
}