use std::collections::HashMap;
use std::io::{Error as IoError};

use crate::{Color, Record};
use crate::filters::{Condition, Expression, Operation, View};
use crate::readers::LogReader;

#[derive(Default)]
struct FilterInner {
    variables_last: HashMap<String, String>,
}

pub struct FilteredLogIterator {
    filter: FilterInner,
    reader: Box<dyn LogReader>,
    view: View,
}

impl FilterInner {
    fn set_variable(&mut self, record: &mut Record, key: String, value: String) {
        record.variables.insert(key.clone(), value.clone());
        self.variables_last.insert(key, value);
    }

    fn evaluate(&self, expression: &Expression, record: &Record) -> String {
        match expression {
            Expression::Record => record.text.to_owned(),
            Expression::Var(name) => record.variables.get(name).cloned().unwrap_or_default(),
            Expression::LastVarValue(name) => self.variables_last.get(name).cloned().unwrap_or_default(),
            Expression::Constant(value) => value.clone(),
        }
    }

    fn apply_operations(
        &mut self,
        record: &mut Record,
        operations: &[Operation],
    ) -> bool {
        for operation in operations {
            match operation {
                Operation::If { condition, then_ops, else_ops } => {
                    match condition {
                        Condition::Match { expression, pattern } => {
                            let value = self.evaluate(expression, record);
                            if let Some(m) =  pattern.match_string(&value) {
                                for (key, value) in m {
                                    self.set_variable(record, key, value);
                                }
                                if !self.apply_operations(record, then_ops) {
                                    return false;
                                }
                            } else {
                                if !self.apply_operations(record, else_ops) {
                                    return false;
                                }
                            }
                        }
                    }
                }
                Operation::Set { target, expression } => {
                    let value = self.evaluate(expression, record);
                    self.set_variable(record, target.to_owned(), value);
                }
                Operation::ColorBy(expression) => {
                    let value = self.evaluate(expression, record);
                    record.color = Color::FromValue { value };
                }
                Operation::SkipRecord => {
                    return false;
                }
            }
        }
        true
    }
}

impl FilteredLogIterator {
    fn try_next(&mut self) -> Result<Option<Record>, IoError> {
        loop {
            let text = match self.reader.read_record()? {
                Some(t) => t,
                None => return Ok(None),
            };
            let mut record = Record::new(text);

            if !self.filter.apply_operations(&mut record, &self.view.operations) {
                continue
            }

            return Ok(Some(record));
        }
    }
}

impl Iterator for FilteredLogIterator {
    type Item = Result<Record, IoError>;

    fn next(&mut self) -> Option<Result<Record, IoError>> {
        match self.try_next() {
            Ok(Some(r)) => Some(Ok(r)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub fn process(reader: Box<dyn LogReader>, view: View) -> FilteredLogIterator {
    FilteredLogIterator {
        filter: Default::default(),
        reader,
        view,
    }
}
