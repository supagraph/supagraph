// working directory of the calling project or tmp if in prod
export const cwd =
  process.env.NODE_ENV === "development"
    ? `${process.cwd()}/data/`
    : "/tmp/data-"; // we should use /tmp/ on prod for an ephemeral store during the execution of this process (max 512mb of space on vercel)

// return withCwd method to concat
export const withCwd = (filePath: string) => {
  return `${cwd}${filePath}`;
};
