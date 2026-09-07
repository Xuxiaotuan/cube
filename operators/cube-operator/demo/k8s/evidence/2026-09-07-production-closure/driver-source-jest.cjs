const root = '/Users/xujiawei/magic/workbench/cube';
module.exports = {
  ...require(root + '/jest.base.config.js'),
  rootDir: root + '/packages/cubejs-cubestore-driver',
  collectCoverage: false,
  testMatch: ['<rootDir>/test/PreAggregationRecovery.test.ts'],
  transform: {
    '^.+\\.tsx?$': [root + '/node_modules/ts-jest', {
      tsconfig: root + '/packages/cubejs-cubestore-driver/tsconfig.json',
      diagnostics: false,
    }],
  },
};
module.exports.moduleNameMapper = {
  ...module.exports.moduleNameMapper,
  '^(\\.{1,2}/.*)\\.js$': '$1',
};
