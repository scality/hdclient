import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig, globalIgnores } from 'eslint/config';
import tseslint from 'typescript-eslint';
import { includeIgnoreFile } from '@eslint/compat';
import scality from '@scality/eslint-config-scality';
import { mocha } from '@scality/eslint-config-scality/testing';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const gitignorePath = path.resolve(__dirname, '.gitignore');

export default defineConfig(
    ...scality,
    ...tseslint.configs.recommended,
    includeIgnoreFile(gitignorePath),
    globalIgnores(['lib/']),
    mocha(['tests/**/*.ts']),
);
