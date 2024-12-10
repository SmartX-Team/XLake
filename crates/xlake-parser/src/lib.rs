::lalrpop_util::lalrpop_mod!(grammar);

pub use self::grammar::SeqParser;

impl ::core::fmt::Debug for SeqParser {
    fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeqParser").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::SeqParser;

    #[test]
    fn test_grammar_simple() {
        let parser = SeqParser::new();
        let input = "stdinsrc ! stdoutsink";
        let _ = parser.parse(input).unwrap();
    }

    #[test]
    fn test_grammar_simple_err() {
        let parser = SeqParser::new();
        let input = "stdin src ! stdout sink";
        assert!(parser.parse(input).is_err());
    }

    #[test]
    fn test_grammar_argument_quote() {
        let parser = SeqParser::new();
        let input = "filesrc path='lib.rs' ! stdoutsink";
        let _ = parser.parse(input).unwrap();
    }

    #[test]
    fn test_grammar_argument_quote_err() {
        let parser = SeqParser::new();
        let input = "filesrc path=lib.rs ! stdoutsink";
        assert!(parser.parse(input).is_err());
    }

    #[test]
    fn test_grammar_argument_sep_comma() {
        let parser = SeqParser::new();
        let input = "filesrc cache=content,path='lib.rs' ! stdoutsink";
        let _ = parser.parse(input).unwrap();
    }

    #[test]
    fn test_grammar_argument_sep_comma_and_space() {
        let parser = SeqParser::new();
        let input = "filesrc cache=content, path='lib.rs' ! stdoutsink";
        let _ = parser.parse(input).unwrap();
    }

    #[test]
    fn test_grammar_argument_sep_space() {
        let parser = SeqParser::new();
        let input = "filesrc cache=content path='lib.rs' ! stdoutsink";
        let _ = parser.parse(input).unwrap();
    }
}
