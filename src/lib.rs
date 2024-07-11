// use anyhow::anyhow;
use log::{debug, error};
use rusqlite::Connection;

use std::convert::TryFrom;

#[derive(Debug)]
pub enum QueryType {
    Author,
    Serie,
}

#[derive(Debug)]
pub struct OpdsApi {
    conn: Connection,
}
impl TryFrom<&str> for OpdsApi {
    type Error = anyhow::Error;

    fn try_from(database: &str) -> anyhow::Result<Self> {
        debug!("database: {database}");
        let conn = Connection::open(&database).inspect_err(|e| error!("{e}"))?;
        Ok(Self { conn })
    }
}
impl TryFrom<&String> for OpdsApi {
    type Error = anyhow::Error;

    fn try_from(database: &String) -> anyhow::Result<Self> {
        debug!("database: {database}");
        OpdsApi::try_from(database.as_str())
    }
}

impl OpdsApi {
    pub fn is_readonly(&self) -> anyhow::Result<bool> {
        Ok(self.conn.is_readonly(rusqlite::DatabaseName::Main)?)
    }


}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() -> anyhow::Result<()>{
        let api = OpdsApi::try_from("file:/lib.rus.ec/books.db?mode=ro")?;
        assert!(api.is_readonly()?);
        Ok(())
    }
}
