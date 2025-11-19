use pyo3::prelude::*;

pub(crate) fn py_to_offset_datetime(
    py_obj: &Bound<pyo3::types::PyAny>
) -> anyhow::Result<time::OffsetDateTime> {

    let dt = match py_obj.cast::<pyo3::types::PyDateTime>() {
        Ok(dt) => dt,
        Err(_) => anyhow::bail!("Expected datetime")
    };

    let year: i32 = dt.getattr("year")?.extract()?;
    let month: u8 = dt.getattr("month")?.extract()?;
    let day: u8 = dt.getattr("day")?.extract()?;

    let hour: u8 = dt.getattr("hour")?.extract()?;
    let minute: u8 = dt.getattr("minute")?.extract()?;
    let second: u8 = dt.getattr("second")?.extract()?;

    let month= time::Month::try_from(month)?;

    let naive_dt = time::PrimitiveDateTime::new(
        time::Date::from_calendar_date(year, month, day)?,
        time::Time::from_hms(hour, minute, second)?
    );
    Ok(naive_dt.assume_utc())
}
