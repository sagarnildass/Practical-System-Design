#!/usr/bin/env python3
"""
Test script for the News Feed API.

This script creates a test scenario by making multiple API calls to:
- Create users
- Create posts
- Set up relationships (follow/friend)
- Perform actions (like, comment, share)
- Test feed functionality
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime
import requests

# API base URL
BASE_URL = "http://localhost:8000/api"

# Test data
TEST_USERS = [
    {"username": "alice", "email": "alice@example.com", "profile_picture_url": "https://randomuser.me/api/portraits/women/1.jpg"},
    {"username": "bob", "email": "bob@example.com", "profile_picture_url": "https://randomuser.me/api/portraits/men/1.jpg"},
    {"username": "charlie", "email": "charlie@example.com", "profile_picture_url": "https://randomuser.me/api/portraits/men/2.jpg"},
    {"username": "diana", "email": "diana@example.com", "profile_picture_url": "https://randomuser.me/api/portraits/women/2.jpg"},
    {"username": "emma", "email": "emma@example.com", "profile_picture_url": "https://randomuser.me/api/portraits/women/3.jpg"},
]

TEST_POSTS = [
    "Just had the best coffee ever! ☕️ #morningvibes",
    "Working on a new project. So excited to share it soon! 🚀",
    "Beautiful sunset today. Nature is amazing! 🌅",
    "Just finished reading an amazing book. Highly recommend! 📚",
    "Friday night movie marathon with friends! 🎬 #weekendvibes",
    "Tried a new recipe today and it turned out perfect! 🍲",
    "Morning run done! Starting the day right! 💪",
    "Just adopted the cutest kitten ever! 😻",
    "Road trip planned for next week! Can't wait! 🚗",
    "New personal best at the gym today! 💯",
]

TEST_COMMENTS = [
    "Great post! 👍",
    "I totally agree!",
    "Thanks for sharing!",
    "This is awesome!",
    "Interesting perspective!",
    "Made my day!",
    "Couldn't agree more!",
    "This is so true!",
    "Love this! 💖",
    "Keep it up!",
]


class NewsApiTest:
    def __init__(self, base_url=BASE_URL):
        self.base_url = base_url
        self.users = []
        self.posts = []
        self.verbose = False
        self.stats = {
            "users_created": 0,
            "posts_created": 0,
            "follows_created": 0,
            "likes_created": 0,
            "comments_created": 0,
            "shares_created": 0,
        }
    
    def set_verbose(self, verbose):
        """Set verbose output."""
        self.verbose = verbose
    
    def log(self, message):
        """Log a message if verbose is enabled."""
        if self.verbose:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
    
    def check_health(self):
        """Check API health."""
        url = f"{self.base_url}/health"
        response = requests.get(url)
        
        if response.status_code == 200:
            health_data = response.json()
            healthy = health_data["status"] == "ok"
            self.log(f"API Health: {health_data}")
            return healthy
        else:
            self.log(f"Health check failed: {response.status_code}")
            return False
    
    def create_users(self):
        """Create test users."""
        self.log("Creating test users...")
        
        for user_data in TEST_USERS:
            url = f"{self.base_url}/users"
            response = requests.post(url, json=user_data)
            
            if response.status_code == 201:
                user = response.json()
                self.users.append(user)
                self.stats["users_created"] += 1
                self.log(f"Created user: {user['username']} ({user['user_id']})")
            else:
                self.log(f"Failed to create user {user_data['username']}: {response.status_code}")
                # Try to get the user if it already exists
                try:
                    response = requests.get(f"{self.base_url}/users/by-username/{user_data['username']}")
                    if response.status_code == 200:
                        user = response.json()
                        self.users.append(user)
                        self.log(f"User already exists: {user['username']} ({user['user_id']})")
                except:
                    pass
    
    def create_follows(self, follow_probability=0.7):
        """Create follow relationships between users."""
        self.log("Creating follow relationships...")
        
        for user in self.users:
            for potential_friend in self.users:
                # Don't follow self
                if user["user_id"] == potential_friend["user_id"]:
                    continue
                    
                # Random chance to follow
                if random.random() > follow_probability:
                    continue
                    
                url = f"{self.base_url}/users/{potential_friend['user_id']}/follow"
                headers = {"X-User-ID": user["user_id"]}
                
                response = requests.post(url, headers=headers)
                
                if response.status_code == 200:
                    self.stats["follows_created"] += 1
                    self.log(f"{user['username']} followed {potential_friend['username']}")
                else:
                    self.log(f"Failed to create follow: {response.status_code}")
    
    def create_posts(self, posts_per_user=2):
        """Create posts for each user."""
        self.log("Creating posts...")
        
        for user in self.users:
            for _ in range(posts_per_user):
                # Pick a random post content
                content = random.choice(TEST_POSTS)
                
                url = f"{self.base_url}/posts"
                headers = {"X-User-ID": user["user_id"]}
                post_data = {
                    "content": content,
                    "post_type": "TEXT"
                }
                
                response = requests.post(url, headers=headers, json=post_data)
                
                if response.status_code == 201:
                    post = response.json()
                    self.posts.append(post)
                    self.stats["posts_created"] += 1
                    self.log(f"{user['username']} created post: {post['post_id']}")
                    
                    # Wait a bit to create time separation between posts
                    time.sleep(0.2)
                else:
                    self.log(f"Failed to create post: {response.status_code}")
    
    def create_likes(self, like_probability=0.5):
        """Create likes for posts."""
        self.log("Creating likes...")
        
        for user in self.users:
            for post in self.posts:
                # Don't like own posts
                if user["user_id"] == post["user_id"]:
                    continue
                    
                # Random chance to like
                if random.random() > like_probability:
                    continue
                    
                url = f"{self.base_url}/posts/{post['post_id']}/like"
                headers = {"X-User-ID": user["user_id"]}
                
                response = requests.post(url, headers=headers)
                
                if response.status_code == 200:
                    self.stats["likes_created"] += 1
                    self.log(f"{user['username']} liked post by {post['username'] or post['user_id']}")
                else:
                    self.log(f"Failed to create like: {response.status_code}")
    
    def create_comments(self, comment_probability=0.3):
        """Create comments on posts."""
        self.log("Creating comments...")
        
        for user in self.users:
            for post in self.posts:
                # Random chance to comment
                if random.random() > comment_probability:
                    continue
                    
                url = f"{self.base_url}/posts/{post['post_id']}/comment"
                headers = {"X-User-ID": user["user_id"]}
                comment_data = {
                    "content": random.choice(TEST_COMMENTS)
                }
                
                response = requests.post(url, headers=headers, json=comment_data)
                
                if response.status_code == 201:
                    comment = response.json()
                    self.stats["comments_created"] += 1
                    self.log(f"{user['username']} commented on post by {post['username'] or post['user_id']}")
                else:
                    self.log(f"Failed to create comment: {response.status_code}")
    
    def create_shares(self, share_probability=0.2):
        """Create shares of posts."""
        self.log("Creating shares...")
        
        for user in self.users:
            for post in self.posts:
                # Don't share own posts
                if user["user_id"] == post["user_id"]:
                    continue
                    
                # Random chance to share
                if random.random() > share_probability:
                    continue
                    
                url = f"{self.base_url}/posts/{post['post_id']}/share"
                headers = {"X-User-ID": user["user_id"]}
                share_data = {
                    "content": f"Check out this post by @{post['username'] or 'someone'}!"
                }
                
                response = requests.post(url, headers=headers, json=share_data)
                
                if response.status_code == 201:
                    share = response.json()
                    self.stats["shares_created"] += 1
                    self.log(f"{user['username']} shared post by {post['username'] or post['user_id']}")
                else:
                    self.log(f"Failed to create share: {response.status_code}")
    
    def test_feeds(self):
        """Test news feeds for each user."""
        self.log("Testing news feeds...")
        
        feed_stats = {}
        
        # Add retry mechanism with delay to ensure feeds are updated
        max_retries = 3
        retry_delay = 1  # seconds
        
        for retry in range(max_retries):
            empty_feeds = 0
            
            for user in self.users:
                url = f"{self.base_url}/feed"
                headers = {"X-User-ID": user["user_id"]}
                
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    feed = response.json()
                    feed_size = len(feed["posts"])
                    feed_stats[user["username"]] = feed_size
                    
                    if feed_size == 0:
                        empty_feeds += 1
                        
                    self.log(f"{user['username']}'s feed has {feed_size} posts")
                    
                    if feed_size > 0:
                        self.log(f"Sample post in {user['username']}'s feed: {feed['posts'][0]['content'][:30]}...")
                else:
                    self.log(f"Failed to get feed for {user['username']}: {response.status_code}")
            
            # If all feeds have posts, we can stop retrying
            if empty_feeds == 0:
                self.log(f"All feeds have posts after retry {retry+1}")
                break
                
            # If not the last retry, sleep and try again
            if retry < max_retries - 1 and empty_feeds > 0:
                self.log(f"Found {empty_feeds} empty feeds, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
        
        return feed_stats
    
    def run_test_scenario(self):
        """Run the complete test scenario."""
        print("🚀 Starting News Feed API test scenario")
        
        # Check API health
        if not self.check_health():
            print("❌ API health check failed. Aborting test.")
            return False
        
        start_time = time.time()
        
        # Create test data
        self.create_users()
        self.create_follows()
        self.create_posts()
        self.create_likes()
        self.create_comments()
        self.create_shares()
        
        # Test feeds
        feed_stats = self.test_feeds()
        
        execution_time = time.time() - start_time
        
        # Print summary
        print("\n📊 Test Scenario Summary:")
        print(f"✅ Users created: {self.stats['users_created']}")
        print(f"✅ Posts created: {self.stats['posts_created']}")
        print(f"✅ Follows created: {self.stats['follows_created']}")
        print(f"✅ Likes created: {self.stats['likes_created']}")
        print(f"✅ Comments created: {self.stats['comments_created']}")
        print(f"✅ Shares created: {self.stats['shares_created']}")
        
        print("\n📱 Feed Statistics:")
        for username, feed_size in feed_stats.items():
            print(f"  - {username}'s feed: {feed_size} posts")
        
        print(f"\n⏱️ Total execution time: {execution_time:.2f} seconds")
        print("✅ Test scenario completed successfully")
        
        return True


def main():
    parser = argparse.ArgumentParser(description="Test the News Feed API with a realistic scenario")
    parser.add_argument("--url", default=BASE_URL, help="Base URL for the API")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    args = parser.parse_args()
    
    tester = NewsApiTest(args.url)
    tester.set_verbose(args.verbose)
    
    if tester.run_test_scenario():
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main()) 