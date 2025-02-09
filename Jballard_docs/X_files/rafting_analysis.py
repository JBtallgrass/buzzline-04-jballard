import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Define file paths
DATA_FILE = "data/rafting_feedback.csv"
IMAGES_FOLDER = Path("images")  # Updated to save in 'images/'

# Ensure the images folder exists
IMAGES_FOLDER.mkdir(parents=True, exist_ok=True)

# Load Data
df = pd.read_csv(DATA_FILE)

# Convert date to datetime format
df["date"] = pd.to_datetime(df["date"], errors="coerce")

# Handle missing values
df.fillna("Unknown", inplace=True)

# Convert 'is_negative' to binary (0 = positive, 1 = negative)
df["is_negative"] = df["is_negative"].apply(lambda x: 1 if x.lower() == "yes" else 0)

# Extract week number for trend analysis
df["week"] = df["date"].dt.isocalendar().week

# Convert river flow to numeric (handle missing values)
df["river_flow"] = pd.to_numeric(df["river_flow"], errors="coerce")

############################################
# 🔹 1️⃣ Positive vs Negative Feedback Count
############################################
feedback_counts = df["is_negative"].value_counts()

plt.figure(figsize=(6, 4))
sns.barplot(x=feedback_counts.index, y=feedback_counts.values, palette=["green", "red"])
plt.xticks(ticks=[0, 1], labels=["Positive", "Negative"])
plt.xlabel("Feedback Type")
plt.ylabel("Count")
plt.title("Positive vs Negative Feedback")
plt.savefig(IMAGES_FOLDER / "feedback_counts.png")  # Save in 'images/'
plt.close()

############################################
# 🔹 2️⃣ Guide Performance - Top Guides with Most Feedback
############################################
guide_feedback = df["guide"].value_counts().head(10)

plt.figure(figsize=(10, 5))
sns.barplot(x=guide_feedback.index, y=guide_feedback.values, palette="Blues_r")
plt.xlabel("Guide")
plt.ylabel("Feedback Count")
plt.title("Top 10 Guides with Most Feedback")
plt.xticks(rotation=45)
plt.savefig(IMAGES_FOLDER / "guide_feedback.png")  # Save in 'images/'
plt.close()

############################################
# 🔹 3️⃣ Weather vs Negative Feedback
############################################
weather_impact = df.groupby("weather")["is_negative"].mean()

plt.figure(figsize=(12, 5))
sns.barplot(x=weather_impact.index, y=weather_impact.values, palette="coolwarm")
plt.xlabel("Weather Condition")
plt.ylabel("Negative Feedback %")
plt.title("Weather Impact on Negative Feedback")
plt.xticks(rotation=45)
plt.savefig(IMAGES_FOLDER / "weather_vs_feedback.png")  # Save in 'images/'
plt.close()

############################################
# 🔹 4️⃣ River Flow vs Feedback Type
############################################
plt.figure(figsize=(8, 5))
sns.boxplot(x=df["is_negative"], y=df["river_flow"])
plt.xlabel("Feedback Type (0 = Positive, 1 = Negative)")
plt.ylabel("River Flow (CFS)")
plt.title("River Flow vs Feedback")
plt.savefig(IMAGES_FOLDER / "river_flow_vs_feedback.png")  # Save in 'images/'
plt.close()

############################################
# 🔹 5️⃣ Weekly Trends in Feedback
############################################
weekly_feedback = df.groupby(["week", "is_negative"]).size().unstack()

plt.figure(figsize=(10, 5))
weekly_feedback.plot(kind="line", marker="o", figsize=(12, 6))
plt.xlabel("Week Number")
plt.ylabel("Feedback Count")
plt.title("Weekly Feedback Trends")
plt.legend(["Positive", "Negative"])
plt.grid()
plt.savefig(IMAGES_FOLDER / "weekly_trends.png")  # Save in 'images/'
plt.close()

############################################
# 🔹 6️⃣ Save Processed CSV for Further Analysis
############################################
df.to_csv(IMAGES_FOLDER / "processed_rafting_feedback.csv", index=False)

print(f"✅ Analysis complete! Plots saved in {IMAGES_FOLDER}")
