use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;

pub(crate) struct EventFd {
    pub(crate) fd: std::os::fd::OwnedFd
}

impl EventFd {
    pub(crate) fn new() -> anyhow::Result<Self> {
        Ok(EventFd{fd: make_eventfd()?})
    }

    pub(crate) fn make_sender(&self) -> Sender {
        Sender { fd: self.fd.as_raw_fd() }
    }

    pub(crate) fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.fd.as_raw_fd()
    }
}

pub(crate) struct Sender {
    pub(crate) fd: std::os::fd::RawFd
}

impl Drop for Sender{
    fn drop(&mut self) {
        unsafe {
            libc::eventfd_write(self.fd.as_raw_fd(), 1);
        }
    }
}

pub fn make_pair() -> anyhow::Result<(EventFd, Sender)> {
    let event_fd = EventFd::new()?;
    let waiter = Sender{fd: event_fd.fd.as_raw_fd()};
    Ok((event_fd, waiter))
}


fn make_eventfd() -> anyhow::Result<std::os::fd::OwnedFd> {
    let fd = match unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) } {
        rv if rv < 0 => return Err(std::io::Error::last_os_error().into()),
        rv => rv
    };

    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
}
