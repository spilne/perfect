use crate::parser::*;

pub fn rewrite(source: &str) -> String {
    let mut result = source.to_string();
    let mut offset: i64 = 0;

    // rewrite for-comprehensions first
    let fors = find_for_comprehensions(source);
    for comp in &fors {
        if let Some(desugared) = desugar_for(&comp.stmts, &comp.yield_expr) {
            let start = (comp.start as i64 + offset) as usize;
            let end = (comp.end as i64 + offset) as usize;
            result = format!("{}{}{}", &result[..start], desugared, &result[end..]);
            offset += desugared.len() as i64 - (comp.end - comp.start) as i64;
        }
    }

    // then rewrite eff($) blocks
    let effs = find_eff_blocks(&result);
    offset = 0;
    let result_copy = result.clone();
    for block in &effs {
        if let Some(desugared) = desugar_dollar(&block.stmts) {
            let start = (block.start as i64 + offset) as usize;
            let end = (block.end as i64 + offset) as usize;
            result = format!("{}{}{}", &result[..start], desugared, &result[end..]);
            offset += desugared.len() as i64 - (block.end - block.start) as i64;
        }
    }

    result
}

fn desugar_for(stmts: &[ForStmt], yield_expr: &str) -> Option<String> {
    if stmts.is_empty() {
        return None;
    }
    Some(desugar_for_at(stmts, 0, yield_expr))
}

fn desugar_for_at(stmts: &[ForStmt], index: usize, yield_expr: &str) -> String {
    if index >= stmts.len() {
        return format!("succeed({})", yield_expr);
    }

    match &stmts[index] {
        ForStmt::Bind { name, expr } => {
            let rest = desugar_for_at(stmts, index + 1, yield_expr);
            format!("({}).flatMap(({}) => {})", expr, name, rest)
        }
        ForStmt::Discard { expr } => {
            if index == stmts.len() - 1 {
                format!("({}).map(() => {})", expr, yield_expr)
            } else {
                let rest = desugar_for_at(stmts, index + 1, yield_expr);
                format!("({}).flatMap(() => {})", expr, rest)
            }
        }
        ForStmt::Guard { condition } => {
            let rest = desugar_for_at(stmts, index + 1, yield_expr);
            format!("({}) ? {} : fail(\"Guard failed\")", condition, rest)
        }
        ForStmt::Let { name, expr } => {
            let rest = desugar_for_at(stmts, index + 1, yield_expr);
            format!("(({}) => {})({})", name, rest, expr)
        }
    }
}

fn desugar_dollar(stmts: &[DollarStmt]) -> Option<String> {
    if stmts.is_empty() {
        return None;
    }
    Some(desugar_dollar_at(stmts, 0))
}

fn desugar_dollar_at(stmts: &[DollarStmt], index: usize) -> String {
    if index >= stmts.len() {
        return "succeed(undefined)".to_string();
    }

    let is_last = index == stmts.len() - 1;

    match &stmts[index] {
        DollarStmt::Bind { name, expr } => {
            let rest = desugar_dollar_at(stmts, index + 1);
            format!("{}.flatMap(({}) => {})", expr, name, rest)
        }
        DollarStmt::Discard { expr } => {
            if is_last {
                expr.clone()
            } else {
                let rest = desugar_dollar_at(stmts, index + 1);
                format!("{}.flatMap(() => {})", expr, rest)
            }
        }
        DollarStmt::Return { expr } => {
            // check for $(...)
            let trimmed = expr.trim();
            if trimmed.starts_with("$(") && trimmed.ends_with(')') {
                return trimmed[2..trimmed.len() - 1].to_string();
            }
            format!("succeed({})", expr)
        }
        DollarStmt::Raw { code } => {
            let rest = desugar_dollar_at(stmts, index + 1);
            format!("sync(() => {{ {} }}).flatMap(() => {})", code, rest)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_comprehension() {
        let input = r#"const program = for {
  x <- getX()
  y <- getY(x)
} yield x + y"#;
        let output = rewrite(input);
        assert!(output.contains("getX()"), "output: {}", output);
        assert!(output.contains("flatMap"), "output: {}", output);
        assert!(output.contains("getY(x)"), "output: {}", output);
        assert!(output.contains("succeed(x + y)"), "output: {}", output);
    }

    #[test]
    fn test_for_with_guard() {
        let input = r#"const p = for {
  x <- getX()
  if x > 0
} yield x * 2"#;
        let output = rewrite(input);
        assert!(output.contains("x > 0"));
        assert!(output.contains("fail(\"Guard failed\")"));
    }

    #[test]
    fn test_eff_dollar() {
        let input = r#"const p = eff(($) => {
  const x = $(getX())
  const y = $(getY(x))
  return x + y
})"#;
        let output = rewrite(input);
        assert!(output.contains("getX().flatMap"));
        assert!(output.contains("getY(x).flatMap"));
        assert!(output.contains("succeed(x + y)"));
    }

    #[test]
    fn test_preserves_regular_for() {
        let input = "for (let i = 0; i < 10; i++) { console.log(i) }";
        assert_eq!(rewrite(input), input);
    }

    #[test]
    fn test_no_change_plain() {
        let input = "const x = 1 + 2";
        assert_eq!(rewrite(input), input);
    }
}
