use pyo3::prelude::*;

pub(crate) fn py_to_offset_datetime(
    py_obj: &Bound<pyo3::types::PyAny>
) -> anyhow::Result<time::OffsetDateTime> {

    use pyo3::types::PyDateAccess;
    use pyo3::types::PyTimeAccess;
    
    let dt = match py_obj.cast::<pyo3::types::PyDateTime>() {
        Ok(dt) => dt,
        Err(_) => anyhow::bail!("Expected datetime")
    };
    
    let naive_dt = time::PrimitiveDateTime::new(
        time::Date::from_calendar_date(
            dt.get_year(),
            time::Month::try_from(dt.get_month())?,
            dt.get_day(),
        )?,
        time::Time::from_hms(
            dt.get_hour(),
            dt.get_minute(),
            dt.get_second(),
        )?
    );
    Ok(naive_dt.assume_utc())
}

