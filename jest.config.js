module.exports = {
  roots: ['tests'],
  testMatch: ['**/*.test.ts'],
  transform: {
    '^.+\\.tsx?$': 'ts-jest'
  },
  moduleNameMapper: {
    "^@/(.*)": "<rootDir>/src/$1"
  }
}
