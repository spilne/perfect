use swc_core::{
    common::{Span, Spanned, DUMMY_SP},
    ecma::{
        ast::*,
        visit::{Visit, VisitMut, VisitMutWith, VisitWith},
    },
    plugin::{plugin_transform, proxies::TransformPluginProgramMetadata},
};

// Deep `$` detector — finds the special ident anywhere inside a node
// (call arguments, binary expressions, nested callbacks, loop bodies, …).
// Nested eff(($) => …) calls are already desugared by the time the outer
// block is examined (children-first visit), so any `$` remaining in an
// unsupported position would be emitted as a dangling identifier that
// explodes at runtime. We refuse at build time instead.
struct DollarFinder {
    found: bool,
}

impl Visit for DollarFinder {
    fn visit_ident(&mut self, n: &Ident) {
        if &*n.sym == "$" {
            self.found = true;
        }
    }
}

fn contains_dollar_deep<N: VisitWith<DollarFinder>>(node: &N) -> bool {
    let mut finder = DollarFinder { found: false };
    node.visit_with(&mut finder);
    finder.found
}

fn contains_return(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::Return(_) => true,
        Stmt::Block(b) => b.stmts.iter().any(contains_return),
        Stmt::If(i) => {
            contains_return(&i.cons) || i.alt.as_deref().map_or(false, contains_return)
        }
        _ => false,
    }
}

pub struct PerfectTransformVisitor;

impl PerfectTransformVisitor {
    fn is_eff_dollar_call(&self, expr: &CallExpr) -> bool {
        if let Callee::Expr(callee) = &expr.callee {
            if let Expr::Ident(ident) = callee.as_ref() {
                if &*ident.sym == "eff" && expr.args.len() == 1 {
                    if let Some(arg) = expr.args.first() {
                        return self.is_dollar_arrow_fn(&arg.expr);
                    }
                }
            }
        }
        false
    }

    fn is_dollar_arrow_fn(&self, expr: &Expr) -> bool {
        if let Expr::Arrow(arrow) = expr {
            if arrow.params.len() == 1 {
                if let Pat::Ident(param) = &arrow.params[0] {
                    return &*param.id.sym == "$";
                }
            }
        }
        false
    }

    fn transform_eff_block(&self, arrow: &ArrowExpr) -> Option<Expr> {
        let body = match &*arrow.body {
            ArrowFunctionBody::FunctionBody(block) => &block.stmts,
            ArrowFunctionBody::Expr(e) => {
                if let Some(inner) = self.extract_dollar_from_expr(e) {
                    return Some(inner);
                }
                return Some(self.make_succeed((**e).clone(), e.span()));
            }
        };
        self.desugar_stmts(body, 0)
    }

    fn desugar_stmts(&self, stmts: &[Stmt], index: usize) -> Option<Expr> {
        if index >= stmts.len() {
            return Some(self.make_succeed_undefined(DUMMY_SP));
        }

        let stmt = &stmts[index];
        let stmt_span = stmt_span(stmt);

        if let Some((pat, inner_expr)) = self.extract_dollar_pat_bind(stmt) {
            let rest = self.desugar_stmts(stmts, index + 1)?;
            return Some(self.make_flat_map_pat(inner_expr, pat, rest, stmt_span));
        }

        if let Some(inner_expr) = self.extract_dollar_discard(stmt) {
            if index == stmts.len() - 1 {
                return Some(inner_expr);
            }
            let rest = self.desugar_stmts(stmts, index + 1)?;
            return Some(self.make_flat_map_discard(inner_expr, rest, stmt_span));
        }

        if let Some(ret_expr) = self.extract_return(stmt) {
            if let Some(inner) = self.extract_dollar_from_expr(&ret_expr) {
                return Some(inner);
            }
            if contains_dollar_deep(&ret_expr) {
                panic!(
                    "@perfect/swc-plugin: unsupported $ usage in a return expression — only \
                     `return $(expr)` or a plain `return expr` compile. Bind first: \
                     `const x = $(expr); return f(x)`"
                );
            }
            let span = ret_expr.span();
            return Some(self.make_succeed(ret_expr, span));
        }

        if let Some(expr) = self.try_desugar_if(stmt, stmts, index) {
            return Some(expr);
        }

        // Pure declaration (no $) — must NOT be sync-wrapped as a statement:
        // the binding would be scoped inside the sync arrow, invisible to
        // the rest of the block. Compile each declarator to
        // sync(() => init).flatMap((pat) => rest).
        if let Stmt::Decl(Decl::Var(var_decl)) = stmt {
            if !contains_dollar_deep(stmt) {
                let rest = self.desugar_stmts(stmts, index + 1)?;
                let mut acc = rest;
                for decl in var_decl.decls.iter().rev() {
                    let init_expr = decl
                        .init
                        .as_ref()
                        .map(|e| (**e).clone())
                        .unwrap_or_else(|| Expr::Ident(self.make_ident("undefined", stmt_span)));
                    let sync_call = self.make_sync_expr(init_expr, stmt_span);
                    acc = self.make_flat_map_pat(sync_call, decl.name.clone(), acc, stmt_span);
                }
                return Some(acc);
            }
        }

        // From here down the statement is emitted inside sync(() => …) —
        // any `$` still inside it would be a dangling identifier at runtime.
        if contains_dollar_deep(stmt) {
            panic!(
                "@perfect/swc-plugin: unsupported $ usage inside eff(($) => …) — $ compiles only \
                 in `const x = $(expr)`, bare `$(expr)`, `return $(expr)`, and if/else branches. \
                 $ inside larger expressions, call arguments, loops, try/switch, or callbacks is \
                 not supported: bind first (`const x = $(e)`) and use the plain value"
            );
        }

        if let Stmt::Expr(expr_stmt) = stmt {
            let rest = self.desugar_stmts(stmts, index + 1)?;
            return Some(self.make_sync_then((*expr_stmt.expr).clone(), rest, stmt_span));
        }

        let rest = self.desugar_stmts(stmts, index + 1)?;
        Some(self.make_sync_stmt_then(stmt.clone(), rest, stmt_span))
    }

    fn extract_dollar_pat_bind(&self, stmt: &Stmt) -> Option<(Pat, Expr)> {
        if let Stmt::Decl(Decl::Var(var_decl)) = stmt {
            if var_decl.decls.len() == 1 {
                let decl = &var_decl.decls[0];
                let pat = decl.name.clone();
                match &pat {
                    Pat::Ident(_) | Pat::Object(_) | Pat::Array(_) => {
                        if let Some(init) = &decl.init {
                            if let Some(inner) = self.extract_dollar_from_expr(init) {
                                return Some((pat, inner));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        None
    }

    fn extract_dollar_discard(&self, stmt: &Stmt) -> Option<Expr> {
        if let Stmt::Expr(expr_stmt) = stmt {
            return self.extract_dollar_from_expr(&expr_stmt.expr);
        }
        None
    }

    fn extract_dollar_from_expr(&self, expr: &Expr) -> Option<Expr> {
        if let Expr::Call(call) = expr {
            if let Callee::Expr(callee) = &call.callee {
                if let Expr::Ident(ident) = callee.as_ref() {
                    if &*ident.sym == "$" && call.args.len() == 1 {
                        return Some((*call.args[0].expr).clone());
                    }
                }
            }
        }
        None
    }

    fn extract_return(&self, stmt: &Stmt) -> Option<Expr> {
        if let Stmt::Return(ret) = stmt {
            return ret.arg.as_ref().map(|e| (**e).clone());
        }
        None
    }

    fn try_desugar_if(&self, stmt: &Stmt, all_stmts: &[Stmt], index: usize) -> Option<Expr> {
        let if_stmt = if let Stmt::If(i) = stmt {
            i
        } else {
            return None;
        };

        if !self.block_contains_dollar(&if_stmt.cons) {
            if let Some(alt) = &if_stmt.alt {
                if !self.block_contains_dollar(alt) {
                    return None;
                }
            } else {
                return None;
            }
        }

        if contains_dollar_deep(&*if_stmt.test) {
            panic!(
                "@perfect/swc-plugin: $ inside an if condition is not supported — bind first: \
                 `const c = $(cond); if (c) {{ … }}`"
            );
        }

        // The branches compile to a ternary of effects and the CONTINUATION
        // runs after either branch — an early `return` inside a branch would
        // not skip the following statements. Refuse rather than miscompile.
        if index + 1 < all_stmts.len() {
            let has_return = contains_return(&if_stmt.cons)
                || if_stmt.alt.as_deref().map_or(false, contains_return);
            if has_return {
                panic!(
                    "@perfect/swc-plugin: `return` inside an if branch followed by more \
                     statements is not supported (the return would not skip them) — make the \
                     if/else the last statement, or restructure with a bound value"
                );
            }
        }

        let if_span = if_stmt.span;
        let then_eff = self.desugar_block_as_eff(&if_stmt.cons);
        let else_eff = match &if_stmt.alt {
            Some(alt) => self.desugar_block_as_eff(alt),
            None => self.make_succeed_undefined(if_span),
        };

        let cond_eff = self.make_sync_expr((*if_stmt.test).clone(), if_stmt.test.span());
        let ternary = Expr::Cond(CondExpr {
            span: if_span,
            test: Box::new(Expr::Ident(self.make_ident("__c", if_stmt.test.span()))),
            cons: Box::new(then_eff),
            alt: Box::new(else_eff),
        });
        let branch = self.make_flat_map(cond_eff, "__c", ternary, if_span);

        if index + 1 >= all_stmts.len() {
            return Some(branch);
        }
        let rest = self.desugar_stmts(all_stmts, index + 1)?;
        Some(self.make_flat_map_discard(branch, rest, if_span))
    }

    fn block_contains_dollar(&self, stmt: &Stmt) -> bool {
        match stmt {
            Stmt::Block(b) => b.stmts.iter().any(|s| self.block_contains_dollar(s)),
            Stmt::Expr(e) => self.expr_contains_dollar(&e.expr),
            Stmt::Return(r) => r
                .arg
                .as_ref()
                .map_or(false, |e| self.expr_contains_dollar(e)),
            Stmt::Decl(Decl::Var(v)) => v.decls.iter().any(|d| {
                d.init
                    .as_ref()
                    .map_or(false, |e| self.expr_contains_dollar(e))
            }),
            Stmt::If(i) => {
                self.block_contains_dollar(&i.cons)
                    || i.alt
                        .as_ref()
                        .map_or(false, |a| self.block_contains_dollar(a))
            }
            _ => false,
        }
    }

    fn expr_contains_dollar(&self, expr: &Expr) -> bool {
        if let Expr::Call(call) = expr {
            if let Callee::Expr(callee) = &call.callee {
                if let Expr::Ident(ident) = callee.as_ref() {
                    if &*ident.sym == "$" {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn desugar_block_as_eff(&self, stmt: &Stmt) -> Expr {
        match stmt {
            Stmt::Block(b) => self
                .desugar_stmts(&b.stmts, 0)
                .unwrap_or_else(|| self.make_succeed_undefined(b.span)),
            _ => {
                let span = stmt_span(stmt);
                self.desugar_stmts(std::slice::from_ref(stmt), 0)
                    .unwrap_or_else(|| self.make_succeed_undefined(span))
            }
        }
    }

    fn make_ident(&self, name: &str, span: Span) -> Ident {
        Ident::new_no_ctxt(name.into(), span)
    }

    fn make_ident_name(&self, name: &str, span: Span) -> IdentName {
        IdentName::new(name.into(), span)
    }

    fn make_flat_map(&self, source: Expr, param_name: &str, body: Expr, span: Span) -> Expr {
        self.make_flat_map_pat(
            source,
            Pat::Ident(BindingIdent {
                id: self.make_ident(param_name, span),
                type_ann: None,
            }),
            body,
            span,
        )
    }

    fn make_flat_map_pat(&self, source: Expr, param: Pat, body: Expr, span: Span) -> Expr {
        Expr::Call(CallExpr {
            span,
            callee: Callee::Expr(Box::new(Expr::Member(MemberExpr {
                span,
                obj: Box::new(source),
                prop: MemberProp::Ident(self.make_ident_name("flatMap", span)),
            }))),
            args: vec![ExprOrSpread {
                spread: None,
                expr: Box::new(Expr::Arrow(ArrowExpr {
                    span,
                    params: vec![param],
                    body: Box::new(ArrowFunctionBody::Expr(Box::new(body))),
                    is_async: false,
                    is_generator: false,
                    type_params: None,
                    return_type: None,
                    ctxt: Default::default(),
                })),
            }],
            type_args: None,
            ctxt: Default::default(),
        })
    }

    fn make_flat_map_discard(&self, source: Expr, body: Expr, span: Span) -> Expr {
        Expr::Call(CallExpr {
            span,
            callee: Callee::Expr(Box::new(Expr::Member(MemberExpr {
                span,
                obj: Box::new(source),
                prop: MemberProp::Ident(self.make_ident_name("flatMap", span)),
            }))),
            args: vec![ExprOrSpread {
                spread: None,
                expr: Box::new(Expr::Arrow(ArrowExpr {
                    span,
                    params: vec![],
                    body: Box::new(ArrowFunctionBody::Expr(Box::new(body))),
                    is_async: false,
                    is_generator: false,
                    type_params: None,
                    return_type: None,
                    ctxt: Default::default(),
                })),
            }],
            type_args: None,
            ctxt: Default::default(),
        })
    }

    fn make_succeed(&self, expr: Expr, span: Span) -> Expr {
        Expr::Call(CallExpr {
            span,
            callee: Callee::Expr(Box::new(Expr::Ident(self.make_ident("succeed", span)))),
            args: vec![ExprOrSpread {
                spread: None,
                expr: Box::new(expr),
            }],
            type_args: None,
            ctxt: Default::default(),
        })
    }

    fn make_succeed_undefined(&self, span: Span) -> Expr {
        self.make_succeed(Expr::Ident(self.make_ident("undefined", span)), span)
    }

    fn make_sync_expr(&self, expr: Expr, span: Span) -> Expr {
        Expr::Call(CallExpr {
            span,
            callee: Callee::Expr(Box::new(Expr::Ident(self.make_ident("sync", span)))),
            args: vec![ExprOrSpread {
                spread: None,
                expr: Box::new(Expr::Arrow(ArrowExpr {
                    span,
                    params: vec![],
                    body: Box::new(ArrowFunctionBody::Expr(Box::new(expr))),
                    is_async: false,
                    is_generator: false,
                    type_params: None,
                    return_type: None,
                    ctxt: Default::default(),
                })),
            }],
            type_args: None,
            ctxt: Default::default(),
        })
    }

    fn make_sync_then(&self, expr: Expr, then: Expr, span: Span) -> Expr {
        let inner_span = expr.span();
        let sync_call = self.make_sync_expr(expr, inner_span);
        self.make_flat_map_discard(sync_call, then, span)
    }

    fn make_sync_stmt_then(&self, stmt: Stmt, then: Expr, span: Span) -> Expr {
        let sync_call = Expr::Call(CallExpr {
            span,
            callee: Callee::Expr(Box::new(Expr::Ident(self.make_ident("sync", span)))),
            args: vec![ExprOrSpread {
                spread: None,
                expr: Box::new(Expr::Arrow(ArrowExpr {
                    span,
                    params: vec![],
                    body: Box::new(ArrowFunctionBody::FunctionBody(FunctionBody {
                        span,
                        stmts: vec![stmt],
                    })),
                    is_async: false,
                    is_generator: false,
                    type_params: None,
                    return_type: None,
                    ctxt: Default::default(),
                })),
            }],
            type_args: None,
            ctxt: Default::default(),
        });
        self.make_flat_map_discard(sync_call, then, span)
    }
}

fn stmt_span(stmt: &Stmt) -> Span {
    match stmt {
        Stmt::Block(s) => s.span,
        Stmt::Empty(s) => s.span,
        Stmt::Debugger(s) => s.span,
        Stmt::With(s) => s.span,
        Stmt::Return(s) => s.span,
        Stmt::Labeled(s) => s.span,
        Stmt::Break(s) => s.span,
        Stmt::Continue(s) => s.span,
        Stmt::If(s) => s.span,
        Stmt::Switch(s) => s.span,
        Stmt::Throw(s) => s.span,
        Stmt::Try(s) => s.span,
        Stmt::While(s) => s.span,
        Stmt::DoWhile(s) => s.span,
        Stmt::For(s) => s.span,
        Stmt::ForIn(s) => s.span,
        Stmt::ForOf(s) => s.span,
        Stmt::Decl(Decl::Var(v)) => v.span,
        Stmt::Decl(Decl::Fn(f)) => f.function.span,
        Stmt::Decl(Decl::Class(c)) => c.class.span,
        Stmt::Decl(_) => DUMMY_SP,
        Stmt::Expr(e) => e.span,
    }
}

impl VisitMut for PerfectTransformVisitor {
    fn visit_mut_expr(&mut self, expr: &mut Expr) {
        expr.visit_mut_children_with(self);

        if let Expr::Call(call) = expr {
            if self.is_eff_dollar_call(call) {
                if let Some(arg) = call.args.first() {
                    if let Expr::Arrow(arrow) = arg.expr.as_ref() {
                        if let Some(transformed) = self.transform_eff_block(arrow) {
                            *expr = transformed;
                        }
                    }
                }
            }
        }
    }
}

#[plugin_transform]
pub fn process_transform(
    mut program: Program,
    _metadata: TransformPluginProgramMetadata,
) -> Program {
    let mut visitor = PerfectTransformVisitor;
    program.visit_mut_with(&mut visitor);
    program
}

#[cfg(test)]
mod tests;
