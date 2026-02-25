export type FunctionDependencyOverrides<TDependencies extends object> = {
  [Name in keyof TDependencies]?: TDependencies[Name] | undefined;
};

export interface FunctionDependencyFactoryInput<
  TDependencies extends object,
  TFactory
> {
  targetName: string;
  defaults: TDependencies;
  overrides?: FunctionDependencyOverrides<TDependencies>;
  create(dependencies: TDependencies): TFactory;
}

function hasOwnProperty(target: object, propertyName: string): boolean {
  return Object.prototype.hasOwnProperty.call(target, propertyName);
}

export class FunctionDependencyBuilder<TDependencies extends object> {
  private readonly dependencies: TDependencies;

  constructor(
    private readonly targetName: string,
    defaults: TDependencies
  ) {
    this.dependencies = { ...defaults };
  }

  withOverrides(overrides: FunctionDependencyOverrides<TDependencies>): this {
    const entries = Object.entries(overrides) as Array<
      [string, TDependencies[keyof TDependencies] | undefined]
    >;

    for (const [overrideName, override] of entries) {
      if (override === undefined) {
        continue;
      }
      if (!hasOwnProperty(this.dependencies, overrideName)) {
        throw new Error(`${this.targetName} override "${overrideName}" is not supported`);
      }
      if (typeof override !== "function") {
        throw new Error(`${this.targetName} override "${overrideName}" must be a function`);
      }

      const dependencyName = overrideName as keyof TDependencies;
      this.dependencies[dependencyName] = override;
    }

    return this;
  }

  build(): TDependencies {
    return { ...this.dependencies };
  }
}

export function buildFunctionDependencies<TDependencies extends object>(
  targetName: string,
  defaults: TDependencies,
  overrides: FunctionDependencyOverrides<TDependencies> = {}
): TDependencies {
  return new FunctionDependencyBuilder(targetName, defaults)
    .withOverrides(overrides)
    .build();
}

export function createFunctionDependencyFactory<
  TDependencies extends object,
  TFactory
>(input: FunctionDependencyFactoryInput<TDependencies, TFactory>): TFactory {
  return input.create(
    buildFunctionDependencies(
      input.targetName,
      input.defaults,
      input.overrides ?? {}
    )
  );
}
