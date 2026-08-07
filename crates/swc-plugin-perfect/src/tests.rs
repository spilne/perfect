use crate::PerfectTransformVisitor;
use swc_core::{
    common::{sync::Lrc, FileName, SourceMap, DUMMY_SP},
    ecma::{
        ast::{CallExpr, Expr, Program},
        codegen::{text_writer::JsWriter, Config, Emitter},
        parser::{lexer::Lexer, Parser, StringInput, Syntax, TsSyntax},
        visit::{Visit, VisitMutWith, VisitWith},
    },
};

fn transform(src: &str) -> String {
    let cm: Lrc<SourceMap> = Default::default();
    let fm = cm.new_source_file(Lrc::new(FileName::Anon), src.to_string());

    let lexer = Lexer::new(
        Syntax::Typescript(TsSyntax::default()),
        Default::default(),
        StringInput::from(&*fm),
        None,
    );
    let mut parser = Parser::new_from(lexer);
    let module = parser.parse_module().expect("parse");
    let mut program = Program::Module(module);

    let mut visitor = PerfectTransformVisitor;
    program.visit_mut_with(&mut visitor);

    let mut buf = vec![];
    {
        let mut emitter = Emitter {
            cfg: Config::default().with_minify(false),
            cm: cm.clone(),
            comments: None,
            wr: JsWriter::new(cm, "\n", &mut buf, None),
        };
        match &program {
            Program::Module(m) => emitter.emit_module(m).expect("emit"),
            Program::Script(s) => emitter.emit_script(s).expect("emit"),
        }
    }
    String::from_utf8(buf).unwrap()
}

fn assert_contains(src: &str, needle: &str) {
    let out = transform(src);
    assert!(
        out.contains(needle),
        "expected output to contain `{}`\n---- input ----\n{}\n---- output ----\n{}",
        needle,
        src,
        out,
    );
}

// ── baseline behaviour (lock what already works) ───────────────────

#[test]
fn binds_an_identifier() {
    assert_contains("eff(($) => { const x = $(e); return x })", "e.flatMap(");
}

#[test]
fn discards_a_bind() {
    assert_contains("eff(($) => { $(log.info(\"hi\")); return 1 })", ".flatMap(");
}

#[test]
fn last_discard_is_the_tail() {
    let out = transform("eff(($) => { $(e) })");
    // trailing `$(e)` at tail becomes the bare expression, not a succeed()
    assert!(
        out.contains("eff") == false || out.contains("succeed(undefined)") == false,
        "unexpected wrapping: {}",
        out
    );
}

#[test]
fn return_expr_wraps_in_succeed() {
    assert_contains("eff(($) => { return 42 })", "succeed(42)");
}

#[test]
fn return_dollar_unwraps() {
    let out = transform("eff(($) => { return $(e) })");
    assert!(
        !out.contains("succeed"),
        "expected no succeed wrapper, got: {}",
        out
    );
}

#[test]
fn plain_statement_becomes_sync() {
    assert_contains("eff(($) => { console.log('hi'); return 1 })", "sync(");
}

#[test]
fn nested_eff_is_transformed() {
    let out = transform("eff(($) => { const x = $(eff(($) => { return 1 })); return x })");
    assert!(
        out.matches("flatMap").count() >= 1 && out.contains("succeed(1)"),
        "expected inner eff to be desugared: {}",
        out,
    );
}

// ── destructuring ──────────────────────────────────────────────────

#[test]
fn object_destructuring_bind() {
    let out = transform("eff(($) => { const { a, b } = $(e); return a + b })");
    assert!(out.contains("flatMap") && out.contains("{"), "got: {}", out);
    assert!(
        out.contains("a") && out.contains("b"),
        "destructured names missing: {}",
        out
    );
}

#[test]
fn array_destructuring_bind() {
    let out = transform("eff(($) => { const [x, y] = $(e); return x + y })");
    assert!(out.contains("flatMap") && out.contains("["), "got: {}", out);
    assert!(
        out.contains("[x, y]") || out.contains("[ x, y ]"),
        "array pattern missing: {}",
        out
    );
}

// ── control flow ───────────────────────────────────────────────────

#[test]
fn if_with_dollar_in_then_branch() {
    let out = transform("eff(($) => { if (cond) { const x = $(e); return x } else { return 0 } })");
    // expect a ternary between two effects
    assert!(
        out.contains("?") && out.contains(":"),
        "expected ternary: {}",
        out
    );
    assert!(out.contains("flatMap"), "expected flatMap: {}", out);
}

#[test]
fn if_without_dollar_stays_as_sync() {
    let out = transform("eff(($) => { if (cond) { log('a') } else { log('b') } return 1 })");
    // without $ inside branches, fall through to the sync(() => ...) path
    assert!(out.contains("sync("), "expected sync wrapping: {}", out);
}

// ── shorthand body ─────────────────────────────────────────────────

#[test]
fn expression_body_with_dollar() {
    let out = transform("eff(($) => $(e))");
    assert!(
        !out.contains("succeed"),
        "expected bare expression: {}",
        out
    );
}

#[test]
fn expression_body_without_dollar_is_succeed() {
    let out = transform("eff(($) => 42)");
    assert!(
        out.contains("succeed(42)"),
        "expected succeed wrapper: {}",
        out
    );
}

// ── source maps: generated nodes must have real spans ──────────────

fn transform_program(src: &str) -> Program {
    let cm: Lrc<SourceMap> = Default::default();
    let fm = cm.new_source_file(Lrc::new(FileName::Anon), src.to_string());
    let lexer = Lexer::new(
        Syntax::Typescript(TsSyntax::default()),
        Default::default(),
        StringInput::from(&*fm),
        None,
    );
    let mut parser = Parser::new_from(lexer);
    let module = parser.parse_module().expect("parse");
    let mut program = Program::Module(module);
    let mut visitor = PerfectTransformVisitor;
    program.visit_mut_with(&mut visitor);
    program
}

struct CallSpanCollector(Vec<(String, bool)>);

impl Visit for CallSpanCollector {
    fn visit_call_expr(&mut self, n: &CallExpr) {
        // identify the callee name if it's a bare ident or a member access
        let name = match &n.callee {
            swc_core::ecma::ast::Callee::Expr(e) => match &**e {
                Expr::Ident(i) => i.sym.to_string(),
                Expr::Member(m) => match &m.prop {
                    swc_core::ecma::ast::MemberProp::Ident(i) => i.sym.to_string(),
                    _ => "<member>".to_string(),
                },
                _ => "<expr>".to_string(),
            },
            _ => "<other>".to_string(),
        };
        self.0.push((name, n.span != DUMMY_SP));
        n.visit_children_with(self);
    }
}

#[test]
fn generated_nodes_have_real_spans() {
    let program = transform_program("eff(($) => { const x = $(e); return x })");
    let mut collector = CallSpanCollector(vec![]);
    program.visit_with(&mut collector);

    // We expect: a .flatMap call (generated) and a succeed() call (generated, from `return x`).
    let flat_map = collector.0.iter().find(|(n, _)| n == "flatMap");
    let succeed = collector.0.iter().find(|(n, _)| n == "succeed");

    assert!(
        flat_map.is_some(),
        "no flatMap call found — transform didn't run?"
    );
    assert!(succeed.is_some(), "no succeed call found");

    assert!(
        flat_map.unwrap().1,
        "generated .flatMap call has a DUMMY_SP — source maps will be broken",
    );
    assert!(
        succeed.unwrap().1,
        "generated succeed() call has a DUMMY_SP — source maps will be broken",
    );
}

#[test]
fn preserved_original_expressions_keep_their_spans() {
    // The original `e` inside `$(e)` is cloned verbatim, so its span should survive.
    let program = transform_program("eff(($) => { const x = $(e); return x })");
    struct FindE(Option<swc_core::common::Span>);
    impl Visit for FindE {
        fn visit_ident(&mut self, n: &swc_core::ecma::ast::Ident) {
            if &*n.sym == "e" && self.0.is_none() {
                self.0 = Some(n.span);
            }
        }
    }
    let mut f = FindE(None);
    program.visit_with(&mut f);
    let span = f.0.expect("couldn't find `e`");
    assert!(
        span != DUMMY_SP,
        "original `e` lost its span during transform"
    );
}
