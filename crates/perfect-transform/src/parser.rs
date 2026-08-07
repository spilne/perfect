// Parser for Perfect for-comprehension and eff($) blocks
// Works on raw source text — no dependency on SWC or any JS parser.
// Handles: for { x <- expr; if cond; val y = expr } yield expr
//          eff(($) => { const x = $(expr); return expr })

#[derive(Debug, Clone)]
pub enum ForStmt {
    Bind { name: String, expr: String },
    Discard { expr: String },
    Guard { condition: String },
    Let { name: String, expr: String },
}

#[derive(Debug, Clone)]
pub struct ForComprehension {
    pub stmts: Vec<ForStmt>,
    pub yield_expr: String,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone)]
pub enum DollarStmt {
    Bind { name: String, expr: String },
    Discard { expr: String },
    Return { expr: String },
    Raw { code: String },
}

#[derive(Debug, Clone)]
pub struct EffBlock {
    pub stmts: Vec<DollarStmt>,
    pub start: usize,
    pub end: usize,
}

pub fn find_for_comprehensions(source: &str) -> Vec<ForComprehension> {
    let mut results = Vec::new();
    let bytes = source.as_bytes();
    let mut i = 0;

    while i < bytes.len().saturating_sub(4) {
        // look for "for" followed by optional whitespace and "{"
        if &bytes[i..i.min(bytes.len()).max(i + 3).min(bytes.len())] == b"for" {
            let after_for = skip_whitespace(bytes, i + 3);
            if after_for < bytes.len() && bytes[after_for] == b'{' {
                // check it's not "for (" — that's a JS for-loop
                if let Some(comp) = parse_for_comprehension(source, i, after_for) {
                    results.push(comp);
                    i = results.last().unwrap().end;
                    continue;
                }
            }
        }
        i += 1;
    }

    results
}

pub fn find_eff_blocks(source: &str) -> Vec<EffBlock> {
    let mut results = Vec::new();
    let bytes = source.as_bytes();
    let mut i = 0;

    while i < bytes.len().saturating_sub(6) {
        if &bytes[i..i + 3] == b"eff" {
            let j = skip_whitespace(bytes, i + 3);
            if j < bytes.len() && bytes[j] == b'(' {
                let k = skip_whitespace(bytes, j + 1);
                if k < bytes.len() && bytes[k] == b'(' {
                    // check for ($)
                    let l = skip_whitespace(bytes, k + 1);
                    if l < bytes.len() && bytes[l] == b'$' {
                        if let Some(block) = parse_eff_block(source, i) {
                            let end = block.end;
                            results.push(block);
                            i = end;
                            continue;
                        }
                    }
                }
            }
        }
        i += 1;
    }

    results
}

fn parse_for_comprehension(
    source: &str,
    start: usize,
    brace_start: usize,
) -> Option<ForComprehension> {
    let brace_end = find_matching_brace(source, brace_start)?;
    let body = &source[brace_start + 1..brace_end];

    // look for "yield" after closing brace
    let after = &source[brace_end + 1..];
    let trimmed = after.trim_start();
    if !trimmed.starts_with("yield") {
        return None;
    }
    let ws_len = after.len() - trimmed.len();
    let yield_start = brace_end + 1 + ws_len + 5; // skip whitespace + "yield"
                                                  // skip whitespace after "yield"
    let expr_start = skip_whitespace(source.as_bytes(), yield_start);
    let yield_expr = extract_expression(source, expr_start)?;
    let end = expr_start + yield_expr.len();

    let stmts = parse_for_stmts(body)?;
    if stmts.is_empty() {
        return None;
    }

    Some(ForComprehension {
        stmts,
        yield_expr: yield_expr.trim().to_string(),
        start,
        end,
    })
}

fn parse_for_stmts(body: &str) -> Option<Vec<ForStmt>> {
    let mut stmts = Vec::new();
    for line in split_statements(body) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // x <- expr
        if let Some(arrow_pos) = trimmed.find("<-") {
            let name = trimmed[..arrow_pos].trim().to_string();
            let expr = trimmed[arrow_pos + 2..].trim().to_string();
            if name == "_" {
                stmts.push(ForStmt::Discard { expr });
            } else {
                stmts.push(ForStmt::Bind { name, expr });
            }
            continue;
        }

        // if condition
        if trimmed.starts_with("if ") {
            stmts.push(ForStmt::Guard {
                condition: trimmed[3..].trim().to_string(),
            });
            continue;
        }

        // val x = expr
        if trimmed.starts_with("val ")
            || trimmed.starts_with("let ")
            || trimmed.starts_with("const ")
        {
            let rest = if trimmed.starts_with("val ") {
                &trimmed[4..]
            } else if trimmed.starts_with("let ") {
                &trimmed[4..]
            } else {
                &trimmed[6..]
            };
            if let Some(eq_pos) = rest.find('=') {
                let name = rest[..eq_pos].trim().to_string();
                let expr = rest[eq_pos + 1..].trim().to_string();
                stmts.push(ForStmt::Let { name, expr });
                continue;
            }
        }
    }

    Some(stmts)
}

fn parse_eff_block(source: &str, start: usize) -> Option<EffBlock> {
    // find the opening { of the arrow body
    let rest = &source[start..];
    let brace_pos = rest.find('{')?;
    let abs_brace = start + brace_pos;
    let brace_end = find_matching_brace(source, abs_brace)?;

    let body = &source[abs_brace + 1..brace_end];
    let stmts = parse_dollar_stmts(body)?;

    // end is }) — closing brace + closing paren
    let after = &source[brace_end + 1..];
    let close_paren = after.find(')')? + brace_end + 1 + 1;

    Some(EffBlock {
        stmts,
        start,
        end: close_paren,
    })
}

fn parse_dollar_stmts(body: &str) -> Option<Vec<DollarStmt>> {
    let mut stmts = Vec::new();
    for line in split_statements(body) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // const x = $(expr)
        if (trimmed.starts_with("const ")
            || trimmed.starts_with("let ")
            || trimmed.starts_with("var "))
            && trimmed.contains("$(")
        {
            let eq_pos = trimmed.find('=')?;
            let name_part = if trimmed.starts_with("const ") {
                &trimmed[6..eq_pos]
            } else if trimmed.starts_with("let ") {
                &trimmed[4..eq_pos]
            } else {
                &trimmed[4..eq_pos]
            };
            let name = name_part.trim().to_string();
            let rhs = trimmed[eq_pos + 1..].trim();
            if rhs.starts_with("$(") && rhs.ends_with(')') {
                let expr = rhs[2..rhs.len() - 1].to_string();
                stmts.push(DollarStmt::Bind { name, expr });
                continue;
            }
        }

        // $(expr)
        let stripped = trimmed.trim_end_matches(';');
        if stripped.starts_with("$(") && stripped.ends_with(')') {
            let expr = stripped[2..stripped.len() - 1].to_string();
            stmts.push(DollarStmt::Discard { expr });
            continue;
        }

        // return expr
        if trimmed.starts_with("return ") {
            let expr = trimmed[7..].trim().trim_end_matches(';').to_string();
            stmts.push(DollarStmt::Return { expr });
            continue;
        }

        stmts.push(DollarStmt::Raw {
            code: trimmed.to_string(),
        });
    }

    Some(stmts)
}

fn find_matching_brace(source: &str, open_pos: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let mut depth = 1;
    let mut i = open_pos + 1;
    while i < bytes.len() && depth > 0 {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            b'"' | b'\'' | b'`' => {
                i = skip_string(bytes, i);
                continue;
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn skip_string(bytes: &[u8], start: usize) -> usize {
    let quote = bytes[start];
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 2;
            continue;
        }
        if bytes[i] == quote {
            return i + 1;
        }
        if quote == b'`' && bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            if let Some(end) = find_matching_brace_bytes(bytes, i + 1) {
                i = end + 1;
                continue;
            }
        }
        i += 1;
    }
    i
}

fn find_matching_brace_bytes(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 1;
    let mut i = open + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn skip_whitespace(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    while i < bytes.len()
        && (bytes[i] == b' ' || bytes[i] == b'\t' || bytes[i] == b'\n' || bytes[i] == b'\r')
    {
        i += 1;
    }
    i
}

fn extract_expression(source: &str, start: usize) -> Option<String> {
    let bytes = source.as_bytes();
    let mut i = start;
    let mut depth = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'"' | b'\'' | b'`' => {
                i = skip_string(bytes, i);
                continue;
            }
            b'(' | b'[' | b'{' => {
                depth += 1;
            }
            b')' | b']' | b'}' => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            }
            b'\n' | b';' if depth == 0 => break,
            _ => {}
        }
        i += 1;
    }

    let expr = source[start..i].trim();
    if expr.is_empty() {
        None
    } else {
        Some(expr.to_string())
    }
}

fn split_statements(body: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut current = String::new();
    let bytes = body.as_bytes();
    let mut depth = 0;
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'"' | b'\'' | b'`' => {
                let end = skip_string(bytes, i);
                current.push_str(&body[i..end]);
                i = end;
                continue;
            }
            b'(' | b'[' | b'{' => {
                depth += 1;
                current.push(bytes[i] as char);
            }
            b')' | b']' | b'}' => {
                depth -= 1;
                current.push(bytes[i] as char);
            }
            b'\n' | b';' if depth == 0 => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    stmts.push(trimmed);
                }
                current.clear();
            }
            _ => {
                current.push(bytes[i] as char);
            }
        }
        i += 1;
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        stmts.push(trimmed);
    }

    stmts
}
