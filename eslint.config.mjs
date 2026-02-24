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
  {
    files: ["apps/**/*.ts"],
    rules: {
      "no-restricted-imports": ["warn", {
        paths: [
          {
            name: "@rising-intelligence/shared",
            message:
              "Root shared imports are being deprecated. Import from explicit shared subpaths.",
          },
          {
            name: "kafkajs",
            message:
              "App code must use @rising-intelligence/pipeline transport interfaces instead of kafkajs directly.",
          },
        ],
      }],
    },
  },
  {
    files: ["packages/ops-cli/src/**/*.ts"],
    ignores: ["packages/ops-cli/src/commands/kafka/**/*.ts"],
    rules: {
      "no-restricted-imports": ["warn", {
        paths: [
          {
            name: "@rising-intelligence/shared",
            message:
              "Root shared imports are being deprecated. Import from explicit shared subpaths.",
          },
          {
            name: "kafkajs",
            message:
              "Only ops-cli Kafka admin commands may import kafkajs directly.",
          },
        ],
      }],
    },
  },
];
