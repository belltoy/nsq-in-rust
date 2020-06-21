use std::time::Duration;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;
use futures::{ready, Future, Stream, Sink};
use tokio::time::Delay;

#[pin_project]
pub(crate) struct Reconnect<F, S, M> {
    #[pin]
    state: State<F, S, Delay>,
    strategy: Strategy,
    mk_connection: M,
    attempts: u32,
}

pub enum Strategy {
    Immediat,
    Exponential(Duration),
}

enum State<F, S, D> {
    Idle,
    Delaying(D),
    Connecting(F),
    Connected(S),
}

impl<F, S, M> Reconnect<F, S, M> {
    pub(crate) fn new(strategy: Strategy, mk_connection: M) -> Self {
        Self {
            state: State::Idle,
            strategy,
            mk_connection,
            attempts: 0,
        }
    }

    fn poll_connected<R, E>(&mut self, cx: &mut Context) -> Poll<&mut S>
    where
        S: Stream<Item = Result<R, E>> + Unpin,
        M: Fn() -> F,
        F: Future<Output = Result<S, E>> + Unpin,
    {
        loop {
            let state = match self.state {
                State::Idle => {
                    match self.strategy {
                        Strategy::Immediat => {
                            let fut = (self.mk_connection)();
                            State::Connecting(fut)
                        }
                        Strategy::Exponential(duration) => {
                            State::Delaying(tokio::time::delay_for(Duration::from_secs(duration.as_secs() * self.attempts as u64)))
                        }
                    }
                }
                State::Delaying(ref mut delay) => {
                    ready!(Pin::new(delay).poll(cx));
                    let fut = (self.mk_connection)();
                    State::Connecting(fut)
                }
                State::Connecting(ref mut fut) => {
                    match Pin::new(fut).poll(cx) {
                        Poll::Ready(Ok(conn)) => {
                            self.attempts = 0;
                            State::Connected(conn)
                        }
                        Poll::Ready(Err(_e)) => {
                            self.attempts += 1;
                            State::Idle
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                State::Connected(ref mut conn) => {
                    return Poll::Ready(conn);
                }
            };
            self.state = state;
        }
    }
}

impl<F, S, M, R, E> Stream for Reconnect<F, S, M>
where
    S: Stream<Item = Result<R, E>> + Unpin,
    M: Fn() -> F,
    F: Future<Output = Result<S, E>> + Unpin,
{
    type Item = Result<R, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            let mut conn = ready!(self.poll_connected(cx));
            match ready!(Pin::new(&mut conn).poll_next(cx)) {
                Some(Ok(res)) => {
                    return Poll::Ready(Some(Ok(res)));
                }
                Some(Err(_e)) => {
                    self.attempts += 1;
                    self.state = State::Idle;
                    continue;
                }
                None => {
                    self.attempts += 1;
                    self.state = State::Idle;
                    continue;
                }
            }
        }
    }
}

impl<F, S, M, R, I, E> Sink<I> for Reconnect<F, S, M>
where
    S: Stream<Item = Result<R, E>> + Unpin,
    S: Sink<I, Error = E> + Unpin,
    M: Fn() -> F,
    F: Future<Output = Result<S, E>> + Unpin,
{
    type Error = E;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let conn = ready!(self.poll_connected(cx));
        let _ = ready!(Pin::new(conn).poll_ready(cx));
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let mut conn = match &mut self.state {
            State::Connected(conn) => conn,
            _ => panic!("Wrong state to start_send, must must be preceded by a successful call to poll_ready"),
        };
        Pin::new(&mut conn).start_send(item)?;
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let conn = ready!(self.poll_connected(cx));
        Pin::new(conn).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let conn = ready!(self.poll_connected(cx));
        Pin::new(conn).poll_close(cx)
    }
}
