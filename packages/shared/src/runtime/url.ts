export function redactUrlPassword(url: string, invalidPlaceholder = "<invalid-url>"): string {
  try {
    const parsed = new URL(url);
    if (parsed.password) {
      parsed.password = "***";
    }

    return parsed.toString();
  } catch {
    return invalidPlaceholder;
  }
}
