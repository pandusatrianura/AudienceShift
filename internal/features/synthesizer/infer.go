package synthesizer

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
)

// SynthesizedTypes holds generated code across supported target languages and schemas.
type SynthesizedTypes struct {
	Go         string `json:"go"`
	TypeScript string `json:"typescript"`
	Zod        string `json:"zod"`
}

// Common initialisms that should remain all-uppercase in Go identifiers.
var commonInitialisms = map[string]string{
	"acl":   "ACL",
	"api":   "API",
	"ascii": "ASCII",
	"cpu":   "CPU",
	"css":   "CSS",
	"dns":   "DNS",
	"eof":   "EOF",
	"guid":  "GUID",
	"html":  "HTML",
	"http":  "HTTP",
	"https": "HTTPS",
	"id":    "ID",
	"ip":    "IP",
	"json":  "JSON",
	"lhs":   "LHS",
	"qps":   "QPS",
	"ram":   "RAM",
	"rhs":   "RHS",
	"rpc":   "RPC",
	"sla":   "SLA",
	"smtp":  "SMTP",
	"sql":   "SQL",
	"ssh":   "SSH",
	"tcp":   "TCP",
	"tls":   "TLS",
	"ttl":   "TTL",
	"udp":   "UDP",
	"ui":    "UI",
	"uid":   "UID",
	"uuid":  "UUID",
	"uri":   "URI",
	"url":   "URL",
	"utf8":  "UTF8",
	"vm":    "VM",
	"xml":   "XML",
	"xmpp":  "XMPP",
	"xsrf":  "XSRF",
	"xss":   "XSS",
}

// toPascalCase converts snake_case, kebab-case, or camelCase to idiomatic Go PascalCase.
func toPascalCase(s string) string {
	if s == "" {
		return ""
	}
	words := splitIntoWords(s)
	var b strings.Builder
	for _, w := range words {
		lower := strings.ToLower(w)
		if init, ok := commonInitialisms[lower]; ok {
			b.WriteString(init)
		} else {
			runes := []rune(w)
			if len(runes) > 0 {
				runes[0] = unicode.ToUpper(runes[0])
				for i := 1; i < len(runes); i++ {
					runes[i] = unicode.ToLower(runes[i])
				}
				b.WriteString(string(runes))
			}
		}
	}
	res := b.String()
	if res == "" {
		return "Field"
	}
	// If first char is a digit, prefix with 'X'
	if unicode.IsDigit(rune(res[0])) {
		res = "X" + res
	}
	return res
}

func splitIntoWords(s string) []string {
	var words []string
	var current strings.Builder

	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if r == '_' || r == '-' || r == '.' || r == ' ' || r == ':' {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
			continue
		}

		if unicode.IsUpper(r) {
			if current.Len() > 0 {
				// Check if followed by lower
				isNextLower := (i+1 < len(runes)) && unicode.IsLower(runes[i+1])
				prevRune := runes[i-1]
				if unicode.IsLower(prevRune) || isNextLower {
					words = append(words, current.String())
					current.Reset()
				}
			}
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		words = append(words, current.String())
	}
	return words
}

// InferredType represents the intermediate type system.
type InferredType struct {
	Kind       string                  // "string", "int", "float", "bool", "time", "null", "array", "object"
	ElemType   *InferredType           // for array
	Fields     map[string]InferredType // for object
	StructName string                  // for named object
}

func inferValue(val interface{}, nameHint string) InferredType {
	if val == nil {
		return InferredType{Kind: "null"}
	}

	switch v := val.(type) {
	case string:
		if _, err := time.Parse(time.RFC3339, v); err == nil && len(v) >= 19 {
			return InferredType{Kind: "time"}
		}
		if _, err := time.Parse(time.RFC3339Nano, v); err == nil && len(v) >= 19 {
			return InferredType{Kind: "time"}
		}
		return InferredType{Kind: "string"}
	case bool:
		return InferredType{Kind: "bool"}
	case float64:
		if float64(int64(v)) == v {
			return InferredType{Kind: "int"}
		}
		return InferredType{Kind: "float"}
	case []interface{}:
		if len(v) == 0 {
			return InferredType{Kind: "array", ElemType: &InferredType{Kind: "null"}}
		}
		elem := inferValue(v[0], nameHint+"Item")
		return InferredType{Kind: "array", ElemType: &elem}
	case map[string]interface{}:
		fields := make(map[string]InferredType, len(v))
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fields[k] = inferValue(v[k], nameHint+toPascalCase(k))
		}
		return InferredType{
			Kind:       "object",
			Fields:     fields,
			StructName: nameHint,
		}
	default:
		return InferredType{Kind: "null"}
	}
}

// Synthesize generates Go structs, TypeScript interfaces, and Zod schemas from raw JSON bytes.
func Synthesize(rootName string, rawJSON []byte) (*SynthesizedTypes, error) {
	if len(strings.TrimSpace(string(rawJSON))) == 0 {
		return nil, errors.New("raw JSON payload is empty")
	}
	if rootName == "" {
		rootName = "WebhookPayload"
	}
	rootName = toPascalCase(rootName)

	var parsed interface{}
	if err := json.Unmarshal(rawJSON, &parsed); err != nil {
		return nil, fmt.Errorf("parsing JSON: %w", err)
	}

	inferred := inferValue(parsed, rootName)

	goCode := generateGo(rootName, inferred)
	tsCode := generateTS(rootName, inferred)
	zodCode := generateZod(rootName, inferred)

	return &SynthesizedTypes{
		Go:         goCode,
		TypeScript: tsCode,
		Zod:        zodCode,
	}, nil
}

// --- Go Generation ---

func generateGo(rootName string, t InferredType) string {
	structs := make(map[string]string)
	hasTime := collectStructsGo(t, structs)

	var b strings.Builder
	if hasTime {
		b.WriteString("import \"time\"\n\n")
	}

	keys := make([]string, 0, len(structs))
	for k := range structs {
		keys = append(keys, k)
	}
	// Ensure root struct comes first, then alphabetized child structs
	sort.Slice(keys, func(i, j int) bool {
		if keys[i] == rootName {
			return true
		}
		if keys[j] == rootName {
			return false
		}
		return keys[i] < keys[j]
	})

	for i, k := range keys {
		if i > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(structs[k])
	}
	return b.String()
}

func collectStructsGo(t InferredType, structs map[string]string) bool {
	hasTime := false
	if t.Kind == "time" {
		return true
	}
	if t.Kind == "array" && t.ElemType != nil {
		if collectStructsGo(*t.ElemType, structs) {
			hasTime = true
		}
	}
	if t.Kind == "object" {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("type %s struct {\n", t.StructName))

		keys := make([]string, 0, len(t.Fields))
		for k := range t.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			field := t.Fields[k]
			fieldName := toPascalCase(k)
			typeName := goTypeString(field)
			if collectStructsGo(field, structs) {
				hasTime = true
			}
			b.WriteString(fmt.Sprintf("\t%-16s %-20s `json:\"%s,omitempty\"`\n", fieldName, typeName, k))
		}
		b.WriteString("}")
		structs[t.StructName] = b.String()
	}
	return hasTime
}

func goTypeString(t InferredType) string {
	switch t.Kind {
	case "string":
		return "string"
	case "int":
		return "int64"
	case "float":
		return "float64"
	case "bool":
		return "bool"
	case "time":
		return "time.Time"
	case "null":
		return "any"
	case "array":
		if t.ElemType == nil {
			return "[]any"
		}
		return "[]" + goTypeString(*t.ElemType)
	case "object":
		return t.StructName
	default:
		return "any"
	}
}

// --- TypeScript Generation ---

func generateTS(rootName string, t InferredType) string {
	interfaces := make(map[string]string)
	collectInterfacesTS(t, interfaces)

	keys := make([]string, 0, len(interfaces))
	for k := range interfaces {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i] == rootName {
			return true
		}
		if keys[j] == rootName {
			return false
		}
		return keys[i] < keys[j]
	})

	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(interfaces[k])
	}
	return b.String()
}

func collectInterfacesTS(t InferredType, interfaces map[string]string) {
	if t.Kind == "array" && t.ElemType != nil {
		collectInterfacesTS(*t.ElemType, interfaces)
	}
	if t.Kind == "object" {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("export interface %s {\n", t.StructName))

		keys := make([]string, 0, len(t.Fields))
		for k := range t.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			field := t.Fields[k]
			collectInterfacesTS(field, interfaces)
			tsType := tsTypeString(field)
			b.WriteString(fmt.Sprintf("  %s?: %s;\n", k, tsType))
		}
		b.WriteString("}")
		interfaces[t.StructName] = b.String()
	}
}

func tsTypeString(t InferredType) string {
	switch t.Kind {
	case "string":
		return "string"
	case "int", "float":
		return "number"
	case "bool":
		return "boolean"
	case "time":
		return "string /* ISO 8601 */"
	case "null":
		return "unknown"
	case "array":
		if t.ElemType == nil {
			return "unknown[]"
		}
		return tsTypeString(*t.ElemType) + "[]"
	case "object":
		return t.StructName
	default:
		return "unknown"
	}
}

// --- Zod Schema Generation ---

func generateZod(rootName string, t InferredType) string {
	schemas := make(map[string]string)
	collectSchemasZod(t, schemas)

	keys := make([]string, 0, len(schemas))
	for k := range schemas {
		keys = append(keys, k)
	}
	// Children first, then root schema
	sort.Slice(keys, func(i, j int) bool {
		if keys[i] == rootName {
			return false
		}
		if keys[j] == rootName {
			return true
		}
		return keys[i] < keys[j]
	})

	var b strings.Builder
	b.WriteString("import { z } from \"zod\";\n\n")

	for i, k := range keys {
		if i > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(schemas[k])
	}

	if t.Kind == "object" {
		b.WriteString(fmt.Sprintf("\n\nexport type %s = z.infer<typeof %sSchema>;", rootName, rootName))
	}
	return b.String()
}

func collectSchemasZod(t InferredType, schemas map[string]string) {
	if t.Kind == "array" && t.ElemType != nil {
		collectSchemasZod(*t.ElemType, schemas)
	}
	if t.Kind == "object" {
		var b strings.Builder
		b.WriteString(fmt.Sprintf("export const %sSchema = z.object({\n", t.StructName))

		keys := make([]string, 0, len(t.Fields))
		for k := range t.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			field := t.Fields[k]
			collectSchemasZod(field, schemas)
			zodType := zodTypeString(field)
			b.WriteString(fmt.Sprintf("  %s: %s.optional(),\n", k, zodType))
		}
		b.WriteString("});")
		schemas[t.StructName] = b.String()
	}
}

func zodTypeString(t InferredType) string {
	switch t.Kind {
	case "string":
		return "z.string()"
	case "int":
		return "z.number().int()"
	case "float":
		return "z.number()"
	case "bool":
		return "z.boolean()"
	case "time":
		return "z.string().datetime()"
	case "null":
		return "z.unknown()"
	case "array":
		if t.ElemType == nil {
			return "z.array(z.unknown())"
		}
		return fmt.Sprintf("z.array(%s)", zodTypeString(*t.ElemType))
	case "object":
		return t.StructName + "Schema"
	default:
		return "z.unknown()"
	}
}
