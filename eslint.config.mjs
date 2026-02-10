import tsParser from "@typescript-eslint/parser";

export default [
  {
    ignores: [
      "**/dist/**",
      "**/node_modules/**",
      "**/coverage/**",
    ],
  },
  {
    files: ["**/*.ts"],
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        ecmaVersion: "latest",
        sourceType: "module",
      },
    },
    rules: {},
  },
];
